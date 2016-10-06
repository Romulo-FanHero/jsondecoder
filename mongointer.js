var mongo = require('mongodb').MongoClient;
var fs = require('fs');

const host = { name: 'localhost', port: 27017 };
const appId = '565c819f3169dd7f607b39c6';
const intermediateFilePath = 'temp/intermediate.json';
const chunkSize = 7500;

function evenly(inc, max) {
    var s = [];
    var c = 0;
    while (c < max) {
        s.push(c);
        c += inc;
    }
    return s;
}

mongo.connect(`mongodb://${host.name}:${host.port}/countly`).then(function(db) {
    db.collection(`app_users${appId}`).find().count().then(function(cnt) {
        var q = [];
        fs.writeFileSync(intermediateFilePath, '[');
        evenly(chunkSize, cnt).forEach(function(offset) {
            var current = 0;
            q.push(db.collection('app_users565c819f3169dd7f607b39c6').find().skip(offset).limit(chunkSize).toArray().then(function(res) {
                console.log('received chunk length:', res.length);
                res.forEach(function(user) {
                    current++;
                    try {
                        fs.appendFileSync(intermediateFilePath,  JSON.stringify(user, null, 2) + ',\n', encoding = 'utf8');
                    }
                    catch (err) {
                        console.error('error:', err);
                        console.log('user:', user);
                    }
                });
            }).catch(function(err) {
                console.error(err);
            }));
        });
        Promise.all(q).then(function() {
            // FIXME extra '\n,' must be manually removed before last ']' after the file is written
            fs.appendFileSync(intermediateFilePath, ']', encoding = 'utf8');
            console.log('processing finished');
            db.close();
        });
    }).catch(function(err) {
        db.close();
        console.log('db error:', err);
    });
}).catch(function(err) {
    console.error('connection errror:', err);
});
