/*jshint unused:strict */

var mongo = require('mongodb').MongoClient,
    fs = require('fs'),
    _ = require('lodash'),
    horoscope = require('horoscope'),
    toAge = require ('to-age'),
    ev = require('email-validator'),
    json2csv = require('json2csv');
//stats = require('stats-lite')
//sleep = require('system-sleep'),

const defaultIntVal = null; // -1
const defaultFloatVal = null; // NaN
const defaultStrVal = null; // 'n/a'
const ANONYMOUS_MODE = true;
const csvOutputFilePath = 'temp/output.csv';

/*
function countEvent(user, eventName) {
    if (!_.has(user, 'pe')) {
        return 0;
    }
    return _.reduce(user.pe, function(n, val) {
        return n + (val === eventName);
    }, 0);
}
*/

function evenly(inc, max) {
    var s = [];
    var c = 0;
    while (c < max) {
        s.push(c);
        c += inc;
    }
    return s;
}

//function orderedJsonStringify(o) {
//    return JSON.stringify(Object.keys(o).sort().reduce((r, k) => (r[k] = o[k], r), {})/*, null, 2*/);
//}

function getFans(params) {
    return mongo.connect(`mongodb://${params.host.name}:${params.host.port}/countly`).then(function(db) {
        return db.collection(`app_users${params.appId}`).find().count().then(function(cnt) {
            var q = [], fans = [];

            last = 1475467; // FIXME

            var fhIds = [];

            evenly(params.chunkSize, cnt).forEach(function(offset) {
                q.push(db.collection('app_users565c819f3169dd7f607b39c6').find().skip(offset).limit(params.chunkSize).toArray().then(function(res) {
                    res.forEach(function proc(user) {
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
                                //fan.screen_height = res[1] > res[0] ? res[1] : res[0];
                                //fan.screen_width = fan.screen_height === res[1] ? res[0] : res[1];
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
                        fan.has_info = _.has(user, 'hasInfo') && user.hasInfo && _.has(user, 'picture') && _.has(user, 'custom.id')  && _.has(user, 'email'); // && isValidUrl(user.picture) FIXME hasInfo might mean more than this
                        fan.name = _.has(user, 'name') ? user.name : defaultStrVal;
                        fan.username = _.has(user, 'username') ? user.username : defaultStrVal;
                        fan.avatar_url = _.has(user, 'picture') ? user.picture : defaultStrVal; // "https://api.fanhero.net/user/${fan.fanheroid}/avatar/thumb-100"
                        fan.email = _.has(user, 'email') ? user.email : defaultStrVal;
                        fan.email_valid = ev.validate(fan.email);
                        fan.email_provider = fan.email_valid ? fan.email.toLowerCase().split('@').pop().split('.').shift() : defaultStrVal;
                        fan.gender = _.has(user, 'gender') ? user.gender : defaultStrVal;

                        // birthday-related stuff
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
                            catch (e) {
                                console.error(e);
                            }
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

                        /*
                        // Event counters
                        fan.msg_count = _.has(user, 'msgs') ? user.msgs.length : 0;
                        fan.event_count = _.has(user, 'pe') ? user.pe.length : 0;
                        fan.alert_count = countEvent(user, 'Alert');
                        fan.authenticate_count = countEvent(user, 'Authenticate');
                        fan.authenticate_error_count = countEvent(user, 'Authenticate Error');
                        fan.edit_profile_count = countEvent(user, 'Edit Profile');
                        fan.edit_profile_error_count = countEvent(user, 'Edit Profile Error');
                        fan.forgot_password_count = countEvent(user, 'Forgot Password');
                        fan.forgot_password_error_count = countEvent(user, 'Forgot Password Error');
                        fan.like_count = countEvent(user, 'Like');
                        fan.livestream_closed_count = countEvent(user, 'Livestream Closed');
                        fan.livestream_opened_count = countEvent(user, 'Livestream Opened');
                        fan.push_opened_count = countEvent(user, '[CLY]_push_open');
                        fan.play_video_count = countEvent(user, 'Play Video');
                        fan.post_comment_count = countEvent(user, 'Post Comment');
                        fan.post_details_count = countEvent(user, 'Post Details');
                        fan.post_like_count = countEvent(user, 'Post Like');
                        fan.register_count = countEvent(user, 'Register');
                        fan.register_error_count = countEvent(user, 'Register Error');
                        fan.sign_out_count = countEvent(user, 'Sign Out');
                        fan.stream_fetch_error_count = countEvent(user, 'Stream Fetch Error');
                        fan.unlike_count = countEvent(user, 'Unlike');
                        fan.zoom_close_count = countEvent(user, 'Zoom Close');

                        // booleans from event counters
                        fan.has_authenticated = fan.authenticate_count > 0;
                        fan.has_commented = fan.post_comment_count > 0;
                        fan.has_edited_profile = fan.edit_profile_count > 0;
                        fan.has_forgot_password = fan.forgot_password_count > 0;
                        fan.has_liked = fan.like_count > 0;
                        fan.has_livestreamed = (fan.livestream_opened_count > 0) || (fan.livestream_closed_count > 0);
                        fan.has_played_video = fan.play_video_count > 0;
                        fan.has_pushed = fan.push_opened_count > 0;
                        fan.has_registered = fan.register_count > 0;
                        fan.has_signed_out = fan.sign_out_count > 0;
                        */

                        if (ANONYMOUS_MODE) {
                            fan = _.omit(fan, ['name', 'username', 'avatar_url', 'email']);
                        }

                        // check if it's a registered user
                        if (_.has(user, 'custom.id') && _.isString(user.custom.id) && (user.custom.id.length > 2)) {

                            // append current fan if its fanheroid is unique
                            if (fhIds.indexOf(user.custom.id) === -1) {
                                fhIds.push(user.custom.id);
                                fans.push(fan);
                            }

                            // if fanheroid is already on the fan list, update counting-related fields
                            else {

                                for (var i = 0, len = fans.length; i < len; i++) {

                                    if (fans[i].fanheroid === user.custom.id) {

                                        var up = {};

                                        // sum session counts
                                        up.session_count = fan.session_count + fans[i].session_count;

                                        // sum total session durations
                                        up.total_session_duration = fan.total_session_duration + fans[i].total_session_duration;

                                        // pick earliest first session timestamp
                                        if (fan.first_session_timestamp !== defaultIntVal) {
                                            if (fans[i].first_session_timestamp !== defaultIntVal) {
                                                up.first_session_timestamp = Math.min(fans[i].first_session_timestamp, fan.first_session_timestamp);
                                            }
                                            else {
                                                up.first_session_timestamp = fan.first_session_timestamp;
                                            }
                                        }
                                        else {
                                            up.first_session_timestamp = fans[i].first_session_timestamp;
                                        }

                                        // pick latest last session timestamp
                                        if (fan.last_session_timestamp !== defaultIntVal) {
                                            if (fans[i].last_session_timestamp !== defaultIntVal) {
                                                up.last_session_timestamp = Math.max(fans[i].last_session_timestamp, fan.last_session_timestamp);
                                            }
                                            else {
                                                up.last_session_timestamp = fan.last_session_timestamp;
                                            }
                                        }
                                        else {
                                            up.last_session_timestamp = fans[i].last_session_timestamp;
                                        }

                                        // recalc seconds since first session
                                        if (up.first_session_timestamp !== defaultIntVal) {
                                            up.seconds_since_first_session = Math.round(last * 1000.0 - up.first_session_timestamp);
                                        }
                                        else {
                                            up.seconds_since_first_session = defaultIntVal;
                                        }

                                        // recalc seconds since last session
                                        if (up.last_session_timestamp !== defaultIntVal) {
                                            up.seconds_since_last_session = Math.round(last * 1000.0 - up.last_session_timestamp);
                                        }
                                        else {
                                            up.seconds_since_last_session = defaultIntVal;
                                        }

                                        // recalc days from first to last session
                                        if ((up.first_session_timestamp !== defaultIntVal) && (up.last_session_timestamp !== defaultIntVal)) {
                                            up.days_from_first_to_last_session = Math.floor((up.last_session_timestamp - up.first_session_timestamp) / 86400.0); // FIXME date subtraction is more correct
                                        }
                                        else {
                                            up.days_from_first_to_last_session = defaultIntVal;
                                        }

                                        // recalc sessions per week
                                        if ((up.first_session_timestamp !== defaultIntVal) && (up.last_session_timestamp !== defaultIntVal) && (up.days_from_first_to_last_session !== defaultIntVal) && (up.days_from_first_to_last_session > 1)) {
                                            up.sessions_per_week = Math.round(fan.session_count * 6.048e8 / (up.last_session_timestamp - up.first_session_timestamp)) / 1.0e3;
                                        }
                                        else {
                                            up.sessions_per_week = defaultIntVal; // or zero
                                        }

                                        // recalc average session duration
                                        if ((up.total_session_duration !== defaultIntVal) && (up.session_count > 1)) {
                                            up.average_session_duration = up.total_session_duration / up.session_count;
                                        }
                                        else {
                                            up.average_session_duration = defaultIntVal; // or zero
                                        }

                                        // prioritize the remainder fields from the fan with the latest session timestamp
                                        if (fan.last_session_timestamp !== defaultIntVal) {
                                            if ((fans[i].last_session_timestamp === defaultIntVal) || (fan.last_session_timestamp > fans[i].last_session_timestamp)) {
                                                fans[i] = fan;
                                            }
                                        }

                                        // update incremental fields
                                        fans[i].session_count = up.session_count;
                                        fans[i].total_session_duration = up.total_session_duration;
                                        fans[i].first_session_timestamp = up.first_session_timestamp;
                                        fans[i].last_session_timestamp = up.last_session_timestamp;
                                        fans[i].seconds_since_first_session = up.seconds_since_first_session;
                                        fans[i].seconds_since_last_session = up.seconds_since_last_session;
                                        fans[i].days_from_first_to_last_session = up.days_from_first_to_last_session;
                                        fans[i].sessions_per_week = up.sessions_per_week;
                                        fans[i].average_session_duration = up.average_session_duration;
                                        break;
                                    }
                                }
                            }
                        }
                        else {
                            // push unregistered fan
                            fans.push(fan);
                        }

                    });
                }).catch(function(err) {
                    console.log(err);
                }));
            });
            return Promise.all(q).then(function() {
                db.close();
                return fans;
            });
        }).catch(function() {
            db.close();
            return [];
        });
    });
}

getFans({
    host: { name: 'localhost', port: 27017 },
    appId: '565c819f3169dd7f607b39c6',
    chunkSize: 500
}).then(function(fans) {
    if (_.isUndefined(fans) || fans.length < 1) {
        console.log('no fans have been returned by the db');
        return;
    }
    try {
        fs.writeFileSync(csvOutputFilePath, json2csv({ data: fans}));
    }
    catch (err) {
        console.error('error while writing csv output file', err);
    }
}).catch(function(err) {
    console.error(err);
});
