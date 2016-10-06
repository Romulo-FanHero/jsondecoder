var mongo = require('mongodb').MongoClient;

function getVisitors(countlyUrl, appId) {
    return mongo.connect(countlyUrl).then(function(db) {
        return db.collection('app_users' + appId).find().count().then(function(cnt) {
            db.close();
            return cnt;
        });
    }).catch(function(err) {
        return -1;
    });
}

getVisitors('mongodb://localhost:27017/countly', '565c819f3169dd7f607b39c6').then(function(cnt) {
    console.log(cnt);
}).catch(function(err) {
    console.error(err);
});
