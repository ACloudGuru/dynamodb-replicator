var Dyno = require('dyno');
var AWS = require('aws-sdk');
var stream = require('stream');
var queue = require('queue-async');
var crypto = require('crypto');
var https = require('https');

module.exports = backfill;

module.exports.agent = new https.Agent({
    keepAlive: true,
    maxSockets: Math.ceil(require('os').cpus().length * 16),
    keepAliveMsecs: 60000
});

function backfill(config, done) {
    config = Object.assign({override: false}, config);

    var s3 = new AWS.S3({
        maxRetries: 1000,
        httpOptions: {
            timeout: 1000,
            agent: module.exports.agent
        }
    });

    var primary = Dyno(config);

    if (config.backup)
        if (!config.backup.bucket || !config.backup.prefix)
            return done(new Error('Must provide a bucket and prefix for incremental backups'));

    primary.describeTable(function(err, data) {
        if (err) return done(err);

        var keys = data.Table.KeySchema.map(function(schema) {
            return schema.AttributeName;
        });

        var count = 0;
        var skipped = 0;
        var starttime = Date.now();

        var writer = new stream.Writable({ objectMode: true, highWaterMark: 1000 });

        writer.queue = queue();
        writer.queue.awaitAll(function(err) { if (err) done(err); });
        writer.pending = 0;

        writer._write = function(record, enc, callback) {
            if (writer.pending > 1000)
                return setImmediate(writer._write.bind(writer), record, enc, callback);

            var key = keys.sort().reduce(function(key, k) {
                key[k] = record[k];
                return key;
            }, {});

            var id = crypto.createHash('md5')
                .update(Dyno.serialize(key))
                .digest('hex');

            var bucket = config.backup.bucket;
            var bucketKey = [config.backup.prefix, config.table, id].join('/');

            writer.drained = false;
            writer.pending++;
            writer.queue.defer(function(next) {
               
                function isBackupMissing() {
                    var getParams = {
                        Bucket: bucket,
                        Key: bucketKey
                    };
                    return s3.getObject(getParams).promise().then(
                        function(data) {
                            return !data;
                        },
                        function(err) {
                            if (err.statusCode === 404) return true;
                            throw err;
                        }
                    );
                }
                
                var determineBackupPermitted = config.override ? Promise.resolve(true) : isBackupMissing();

                determineBackupPermitted
                    .then(function(permitted) {
                        if (permitted) {
                            return s3.putObject({
                                Bucket: bucket,
                                Key: bucketKey,
                                Body: Dyno.serialize(record)
                            }).promise();
                        } else {
                            skipped++;
                        }
                    })
                    .catch(function(err) {
                        writer.emit('error', err);
                    })
                    .then(function() {
                        count++;
                        process.stdout.write('\r\033[K Total: ' + count + ', Skipped: ' + skipped + ' - ' + (count / ((Date.now() - starttime) / 1000)).toFixed(2) + '/s');
                        writer.pending--;
                        next();
                    });
            });
            callback();
        };

        writer.once('error', done);

        var end = writer.end.bind(writer);
        writer.end = function() {
            writer.queue.awaitAll(end);
        };

        primary.scanStream()
            .on('error', next)
            .pipe(writer)
            .on('error', next)
            .on('finish', next);

        function next(err) {
            if (err) return done(err);
            done(null, { count: count });
        }
    });
}
