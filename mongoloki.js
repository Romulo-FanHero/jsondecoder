/*jshint unused:strict */

var mongo = require('mongodb').MongoClient,
    _ = require('lodash'),
    FastSet = require('collections/fast-set'),
    loki = require('lokijs')/*,
    fs = require('fs')*/;

const defaultIntVal = -1;
const defaultFloatVal = null;
const defaultStrVal = null;

const params = {
    host: { name: 'localhost', port: 27017 },
    appId: '565c819f3169dd7f607b39c6',
    chunkSize: 100
};

function evenly(inc, max) {
    var s = [], c = 0;
    while (c < max) {
        s.push(c);
        c += inc;
    }
    return s;
}

function toFan(user) {
    var fan = {};
    fan.session_count = _.has(user, 'sc') && _.isFinite(user.sc) ? user.sc : 0;
    fan.first_session_timestamp = _.has(user, 'fs') && _.isFinite(user.fs) ? user.fs : defaultIntVal;
    fan.last_session_timestamp = _.has(user, 'ls') && _.isFinite(user.ls) ? user.ls : defaultIntVal;
    fan.device_id = _.has(user, 'did') ? user.did : defaultStrVal;
    fan.device_name = _.has(user, 'd') ? user.d : defaultStrVal;
    fan.country_code = _.has(user, 'cc') ? user.cc : defaultStrVal;
    fan.city_name = _.has(user, 'cty') ? user.cty : defaultStrVal;
    fan.latitude = _.has(user, 'lat') ? user.lat : defaultFloatVal;
    fan.longitude = _.has(user, 'lng') ? user.lng : defaultFloatVal;
    fan.locale = _.has(user, 'lo') ? user.lo : defaultStrVal;
    fan.carrier = _.has(user, 'c') ? user.c : defaultStrVal;
    fan.app_version = _.has(user, 'av') ? user.av : defaultStrVal;
    fan.platform = _.has(user, 'p') ? user.p : (_.has(user, 'src') ? user.src : defaultStrVal);
    fan.platform_version = _.has(user, 'pv') ? user.pv : defaultStrVal;
    fan.total_session_duration = _.has(user, 'tsd') ? Math.round(user.tsd) : 0;
    fan.isRegistered = _.has(user, 'custom.id') && _.isString(user.custom.id) && (user.custom.id.length > 2);
    fan.fanheroid = fan.isRegistered ? user.custom.id : defaultStrVal;
    fan._id = _.has(user, '_id') ? user._id : defaultStrVal;
    fan.uid = _.has(user, 'uid') ? user.uid : defaultStrVal;
    fan.push_enabled = (_.has(user, 'tkip') && user.tkip) || (_.has(user, 'tkap') && user.tkap);
    fan.language = _.has(user, 'la') ? user.la : defaultStrVal;
    fan.has_info = _.has(user, 'hasInfo') && user.hasInfo && _.has(user, 'picture') && _.has(user, 'custom.id')  && _.has(user, 'email');
    fan.name = _.has(user, 'name') ? user.name : defaultStrVal;
    fan.username = _.has(user, 'username') ? user.username : defaultStrVal;
    fan.picture = _.has(user, 'picture') ? user.picture : defaultStrVal;
    fan.email = _.has(user, 'email') ? user.email : defaultStrVal;
    fan.gender = _.has(user, 'gender') ? user.gender : defaultStrVal;
    fan.birth_day = defaultIntVal;
    fan.birth_month = defaultIntVal;
    fan.birth_year = _.has(user, 'byear') ? user.byear : defaultIntVal;
    if (_.has(user, 'custom.birthday')) {
        try {
            var date = new Date(user.custom.birthday);
            if (Object.prototype.toString.call(date) === '[object Date]') {
                if (_.isFinite(date.getTime())) {
                    fan.birth_year = date.getFullYear();
                    fan.birth_month = date.getMonth() + 1;
                    fan.birth_day = date.getDate();
                }
            }
        }
        catch (e) {}
    }
    return fan;
}

var lk = new loki('loki.db'/*, {
    autosave: true,
    autosaveInterval: 10000
}*/);
var fans = lk.addCollection('fans', {
    indices: ['_id', 'uid'],
    unique: ['_id', 'uid']
});
//db.addCollection('users', { indices: ['email'] });
mongo.connect(`mongodb://${params.host.name}:${params.host.port}/countly`).then(function(db) {
    db.collection(`app_users${params.appId}`).find().count().then(function(cnt) {
        var q = [];
        var fhids = new FastSet();
        var totalRegistered = 0;
        var totalValid = 0;
        var totalRecords = 0;
        evenly(params.chunkSize, cnt).forEach(function(offset) {
            q.push(db.collection('app_users565c819f3169dd7f607b39c6').find().skip(offset).limit(params.chunkSize).toArray().then(function(res) {
                res.forEach(function proc(user) {
                    totalRecords++;
                    var fan = toFan(user);
                    if (!_.isString(fan._id) || (fan._id.length < 20)) {
                        return; // there should be no _id duplicated but there may be invalid ones
                    }
                    if (!_.isString(fan.uid) || !_.inRange(fan.uid.length, 0, 5)) {
                        return;
                    }
                    if (fan.isRegistered) {
                        if (fhids.has(fan.fanheroid)) {
                            return;
                        }
                        fhids.add(fan.fanheroid);
                        totalRegistered++;
                    }
                    console.log(fan._id, fan.fanheroid);
                    fans.insert(fan);
                    totalValid++;
                });
            }).catch(function(err) {
                console.log(err);
            }));
        });
        Promise.all(q).then(function() {
            console.log('total of unique registered users', totalRegistered);
            console.log('total of valid visitors', totalValid);
            console.log('total of records', totalRecords);
            console.log(fans.chain().find({
                $and: [
                    { isRegistered: { $eq: true } },
                    { birth_year: { $gte: 2000,  $ne: defaultIntVal } }
                ]
            }).count());
            var view = fans.addDynamicView('filtered');
            view.applyFind({ isRegistered: { $eq: true } });
            view.applyFind({ birth_year: { $gte: 1990,  $ne: -1 } });
            //view.applySimpleSort('session_count');
            //var top10 = view.branchResultset().limit(10).data();
            var num = view.branchResultset().mapReduce(function(obj) {
                return obj.session_count;
            }, function(array) {
                return (array[0] + array[array.length - 1]) / 2.0;
            });
            console.log(JSON.stringify(num, null, 2));
            lk.close();
            db.close();
        }).catch(function(err) {
            console.log(err);
            lk.close();
            db.close();
        });
    }).catch(function(err) {
        console.log(err);
        db.close();
        lk.close();
    });
}).catch(function(err) {
    console.log(err);
});
