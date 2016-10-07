/*jshint unused:strict */

var mongo = require('mongodb').MongoClient,
    fs = require('fs'),
    _ = require('lodash'),
    horoscope = require('horoscope'),
    toAge = require ('to-age'),
    ev = require('email-validator');

const defaultIntVal = null;
const defaultFloatVal = null;
const defaultStrVal = null;
const ANONYMOUS_MODE = true;
const jsonOutputFilePath = 'temp/output.json';
const params = {
    host: { name: 'localhost', port: 27017 },
    appId: '565c819f3169dd7f607b39c6',
    chunkSize: 10
};

function evenly(inc, max) {
    var s = [], c = 0;
    while (c < max) {
        s.push(c);
        c += inc;
    }
    return s;
}

mongo.connect(`mongodb://${params.host.name}:${params.host.port}/countly`).then(function(db) {
    db.collection(`app_users${params.appId}`).find().count().then(function(cnt) {
        var q = [], last = Date.now() / 1000, counter = 0, prev = 0;
        var wstream = fs.createWriteStream(jsonOutputFilePath);
        wstream.write('[');
        evenly(params.chunkSize, cnt).forEach(function(offset) {
            q.push(db.collection('app_users565c819f3169dd7f607b39c6').find().skip(offset).limit(params.chunkSize).toArray().then(function(res) {
                res.forEach(function proc(user) {
                    counter++;
                    rnd = Math.round(counter * 1000.0 / cnt) / 10.0;
                    if (rnd !== prev) {
                        console.log(rnd);
                        prev = rnd;
                    }
                    var fan = {};
                    fan.session_count = _.has(user, 'sc') && _.isFinite(user.sc) ? user.sc : 0;
                    fan.first_session_timestamp = _.has(user, 'fs') && _.isFinite(user.fs) ? user.fs : defaultIntVal;
                    fan.last_session_timestamp = _.has(user, 'ls') && _.isFinite(user.ls) ? user.ls : defaultIntVal;
                    fan.seconds_since_first_session = (fan.first_session_timestamp !== defaultIntVal) ? Math.round(last * 1000.0 - user.fs) : defaultIntVal;
                    fan.seconds_since_last_session = (fan.last_session_timestamp !== defaultIntVal) ? Math.round(last * 1000.0 - user.ls) : defaultIntVal;
                    fan.days_from_first_to_last_session = (fan.first_session_timestamp !== defaultIntVal) && (fan.last_session_timestamp !== defaultIntVal) ? Math.round(Math.abs((+new Date(fan.last_session_timestamp * 1000)) - (+new Date(fan.first_session_timestamp * 1000))) / 8.64e7) : defaultIntVal;
                    fan.sessions_per_week = _.has(user, 'sc') && _.has(user, 'fs') && _.has(user, 'ls') && (fan.days_from_first_to_last_session > 5) ? Math.round(fan.session_count * 6.048e8 / (fan.last_session_timestamp - fan.first_session_timestamp)) / 1.0e3 : defaultFloatVal;
                    fan.device_id = _.has(user, 'did') ? user.did : defaultStrVal;
                    fan.device_name = _.has(user, 'd') ? user.d : defaultStrVal;
                    fan.country_code = _.has(user, 'cc') ? user.cc : defaultStrVal;
                    fan.city_name = _.has(user, 'cty') ? user.cty : defaultStrVal;
                    fan.ip_latitude = _.has(user, 'lat') ? user.lat : defaultFloatVal;
                    fan.ip_longitude = _.has(user, 'lng') ? user.lng : defaultFloatVal;
                    fan.gps_latitude = _.has(user, 'loc.geo.coordinates') ? user.loc.geo.coordinates[1] : defaultFloatVal;
                    fan.gps_longitude = _.has(user, 'loc.geo.coordinates') ? user.loc.geo.coordinates[0] : defaultFloatVal;
                    fan.gps_timestamp = _.has(user, 'loc.date') ? Math.round(user.loc.date / 1000.0) : defaultFloatVal;
                    fan.gps_enabled = _.has(user, 'loc.geo');
                    fan.locale = _.has(user, 'lo') ? user.lo : defaultStrVal;
                    fan.carrier = _.has(user, 'c') ? user.c : defaultStrVal;
                    fan.app_version = _.has(user, 'av') ? user.av : defaultStrVal;
                    fan.platform = _.has(user, 'p') ? user.p : (_.has(user, 'src') ? user.src : defaultStrVal);
                    fan.platform_version = _.has(user, 'pv') ? user.pv : defaultStrVal;
                    fan.total_session_duration = _.has(user, 'tsd') ? Math.round(user.tsd) : 0;
                    fan.average_session_duration = _.has(user, 'tsd') && _.has(user, 'sc') && (user.sc > 5) && (user.tsd > 60) ? user.tsd / user.sc : /*defaultIntVal*/ 0.0;
                    fan.location = _.has(user, 'custom.location') ? user.custom.location : defaultStrVal;
                    fan.fanheroid = _.has(user, 'custom.id') && _.isString(user.custom.id) && (user.custom.id.length > 2) ? user.custom.id : defaultStrVal;
                    fan._id = _.has(user, '_id') ? user._id : defaultStrVal;
                    fan.uid = _.has(user, 'uid') ? user.uid : defaultStrVal;
                    fan.push_enabled = (_.has(user, 'tkip') && user.tkip) || (_.has(user, 'tkap') && user.tkap);
                    fan.screen_resolution = _.has(user, 'r') ? user.r : defaultStrVal;
                    fan.screen_height = defaultIntVal;
                    fan.screen_width = defaultIntVal;
                    var res = [];
                    if (_.has(user, 'r')) {
                        res = user.r.match(/\d+/g);
                    }
                    if (res.length === 2) {
                        res[0] = parseInt(res[0]);
                        res[1] = parseInt(res[1]);
                        if (_.isFinite(res[0]) && _.isFinite(res[1])) {
                            fan.screen_height = Math.max(res[0], res[1]);
                            fan.screen_width = Math.min(res[0], res[1]);
                        }
                    }
                    if (fan.screen_width !== defaultIntVal) {
                        var w = fan.screen_width;
                        fan.screen_cat = w < 500 ? 'LOW RES' : fan.screen_cat;
                        fan.screen_cat = (w >= 500) && (w < 680) ? 'SUB HD' : fan.screen_cat;
                        fan.screen_cat = (w >= 680) && (w < 900) ? 'HD' : fan.screen_cat;
                        fan.screen_cat = (w >= 900) && (w < 1260) ? 'FULL HD' : fan.screen_cat;
                        fan.screen_cat = (w >= 1260) && (w < 1700) ? '2K' : fan.screen_cat;
                        fan.screen_cat = (w >= 1700) && (w < 2200) ? '4K' : fan.screen_cat;
                        fan.screen_cat = w >= 2200 ? '>4K' : fan.screen_cat;
                    }
                    else {
                        fan.screen_cat = defaultStrVal;
                    }
                    fan.screen_density = _.has(user, 'dnst') ? user.dnst : defaultStrVal;
                    fan.language = _.has(user, 'la') ? user.la : defaultStrVal;
                    fan.has_info = _.has(user, 'hasInfo') && user.hasInfo && _.has(user, 'picture') && _.has(user, 'custom.id')  && _.has(user, 'email');
                    if (!ANONYMOUS_MODE) {
                        fan.name = _.has(user, 'name') ? user.name : defaultStrVal;
                        fan.username = _.has(user, 'username') ? user.username : defaultStrVal;
                        fan.avatar_url = _.has(user, 'picture') ? user.picture : defaultStrVal;
                        fan.email = _.has(user, 'email') ? user.email : defaultStrVal;
                        fan.email_valid = ev.validate(fan.email);
                    }
                    fan.email_provider = ev.validate(user.email) ? user.email.toLowerCase().split('@').pop().split('.').shift() : defaultStrVal;
                    fan.gender = _.has(user, 'gender') ? user.gender : defaultStrVal;
                    fan.birthdate = defaultStrVal;
                    fan.birth_timestamp = defaultIntVal;
                    fan.seconds_since_birth = defaultIntVal;
                    fan.birth_day = defaultIntVal;
                    fan.birth_month = defaultIntVal;
                    fan.birth_year = _.has(user, 'byear') ? user.byear : defaultIntVal;
                    fan.age = _.has(user, 'byear') ? (new Date().getFullYear() - user.byear) : defaultIntVal;
                    if (_.has(user, 'custom.birthday')) {
                        try {
                            var date = new Date(user.custom.birthday);
                            if (Object.prototype.toString.call(date) === '[object Date]') {
                                if (_.isFinite(date.getTime())) {  // d.valueOf() could also work
                                    fan.birthdate = user.custom.birthday;
                                    fan.birth_timestamp = Math.round(date.getTime() / 1000);
                                    fan.seconds_since_birth = Math.round(last - fan.birth_timestamp);
                                    fan.birth_year = date.getFullYear();
                                    fan.birth_month = date.getMonth() + 1;
                                    fan.birth_day = date.getDate();
                                    fan.age = toAge(date);
                                    fan.astrological_sign = horoscope.getSign({month: fan.birth_month, day: fan.birth_day }, true);
                                }
                            }
                        }
                        catch (e) {}
                    }
                    fan.astrological_zodiac = horoscope.getZodiac(fan.birth_year, true);
                    if (fan.age !== defaultIntVal) {
                        fan.age_range = fan.age < 18 ? '0-17' : fan.age_range;
                        fan.age_range = (fan.age >= 18) && (fan.age < 25) ? '18-24' : fan.age_range;
                        fan.age_range = (fan.age >= 25) && (fan.age < 35) ? '25-34' : fan.age_range;
                        fan.age_range = (fan.age >= 35) && (fan.age < 45) ? '35-44' : fan.age_range;
                        fan.age_range = (fan.age >= 45) && (fan.age < 55) ? '45-54' : fan.age_range;
                        fan.age_range = (fan.age >= 55) && (fan.age < 65) ? '55-64' : fan.age_range;
                        fan.age_range = fan.age >= 65 ? '65+' : fan.age_range;
                    }
                    else {
                        fan.age_range = defaultStrVal;
                    }
                    wstream.write(JSON.stringify(fan, null, 2) + ',\n');
                });
            }).catch(function(err) {
                console.log(err);
            }));
        });
        Promise.all(q).then(function() {
            wstream.write(']');
            wstream.end();
            db.close();
        });
    }).catch(function() {
        db.close();
    });
});
