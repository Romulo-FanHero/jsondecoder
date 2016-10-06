var mongo = require('mongodb').MongoClient,
    fs = require('fs'),
    _ = require('lodash');

function evenly(inc, max) {
    var s = [];
    var c = 0;
    while (c < max) {
        s.push(c);
        c += inc;
    }
    return s;
}

function getUsers(params) {
    return mongo.connect(`mongodb://${params.host.name}:${params.host.port}/countly`).then(function(db) {
        return db.collection(`app_users${params.appId}`).find().count().then(function(cnt) {
            var q = [], users = [];
            evenly(params.chunkSize, cnt).forEach(function(offset) {
                q.push(db.collection('app_users565c819f3169dd7f607b39c6').find().skip(offset).limit(params.chunkSize).toArray().then(function(res) {
                    users = users.concat(res);
                }).catch());
            });
            return Promise.all(q).then(function() {
                db.close();
                return users;
            });
        }).catch(function(err) {
            db.close();
            return [];
        });
    });
}

getUsers({
    host: { name: 'localhost', port: 27017 },
    appId: '565c819f3169dd7f607b39c6',
    chunkSize: 7500
}).then(function(users) {
    if (_.isUndefined(users) || users.length < 1) {
        console.log('no users have been returned by the db');
        return;
    }
    users.forEach(function(user) {
        console.log(user._id);
    });
}).catch(function(err) {
    console.error(err);
});
