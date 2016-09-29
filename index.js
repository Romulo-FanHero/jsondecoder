var _ = require('lodash');
var fs = require('fs');
var horoscope = require('horoscope');
var toAge = require ('to-age');
var ev = require('email-validator');
var json2csv = require('json2csv');
var stats = require('stats-lite');

var _csv2sql = require('csv2sql-lite');
var csv2sql = _csv2sql({
    tableName: 'users',
    dbName: 'dump',
    dropTable: true,
    seperator: ',',
    lineSeperator: '\n'
});

const defaultIntVal = null; // -1
const defaultFloatVal = null; // NaN
const defaultStrVal = null; // 'n/a'

const inputFilePath = 'temp/input.json';
const intermediateFilePath = 'temp/intermediate.json';
const jsonOutputFilePath = 'temp/output.json';
const csvOutputFilePath = 'temp/output.csv';
const sqlOutputFilePath = 'temp/dump.sql';

function countEvent(user, eventName) {
    if (!_.has(user, 'pe')) {
        return 0;
    }
    //console.log(user.pe);
    return _.reduce(user.pe, function(n, val) {
        return n + (val === eventName);
    }, 0);
}

function orderedJsonStringify(o) {
    return JSON.stringify(Object.keys(o).sort().reduce((r, k) => (r[k] = o[k], r), {})/*, null, 2*/);
}

try {
    // check if fixed file already exist, thowing an exception if it doesn't
    fs.accessSync(intermediateFilePath, fs.F_OK);
}
catch (e) {
    // try to create fixed file
    try {
        fs.writeFileSync(intermediateFilePath,
            fs.readFileSync(inputFilePath, 'utf8')
            .split(`/* 1 */`).join(``)
            .split(`ObjectId("`).join(`"`)
            .split(`")`).join(`"`)
        );
    }
    catch (err) {
        console.error('error while trying to fix dump file: ', err);
        process.exit();
    }
}

try {
    // try to parse fixed file
    var users = JSON.parse(fs.readFileSync(intermediateFilePath, 'utf8'));
}
catch (err) {
    console.error('error while parsing fixed file: ', err);
    process.exit();
}

try {

    var fhIds = [];
    var knGdr = [];
    var both = [];
    var sess7 = [];
    var plat = [];
    var cc = [];
    var cty = [];
    var asd = [];
    var regPushCnt = 0;
    var scFromReg = 0;

    // try to decode user array into fan array
    var fans = [];
    users.forEach(function(user) {

        var fan = {};
        fan.session_count = _.has(user, 'sc') ? user.sc : 0;
        fan.first_session_timestamp = _.has(user, 'fs') ? user.fs : defaultIntVal;
        fan.last_session_timestamp = _.has(user, 'ls') ? user.ls : defaultIntVal;
        fan.seconds_since_first_session = _.has(user, 'fs') ? Math.round(Date.now() / 1000 - user.fs) : defaultIntVal;
        fan.seconds_since_last_session = _.has(user, 'ls') ? Math.round(Date.now() / 1000 - user.ls) : defaultIntVal;
        fan.days_from_first_to_last_session = _.has(user, 'fs') && _.has(user, 'ls') ? Math.floor((fan.last_session_timestamp - fan.first_session_timestamp) / 86400.0) : defaultIntVal; // FIXME date subtraction is more correct
        fan.sessions_per_week = _.has(user, 'sc') && _.has(user, 'fs') && _.has(user, 'ls') && (fan.days_from_first_to_last_session > 5) ? fan.session_count * 7.0 / fan.days_from_first_to_last_session : defaultFloatVal;
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
        fan.fanheroid = _.has(user, 'custom.id') ? user.custom.id : defaultStrVal;
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
                        fan.seconds_since_birth = Math.round(Date.now() / 1000 - fan.birth_timestamp);
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

        // ### ANONIMITY DANGER ###
        fan.name = _.has(user, 'name') ? user.name : defaultStrVal;
        fan.username = _.has(user, 'username') ? user.username : defaultStrVal;

        // registered user handling
        if (_.has(user, 'custom.id') && !_.isEmpty(user.custom.id) && _.isString(user.custom.id) && (user.custom.id.length > 2)) {

            if (fhIds.indexOf(user.custom.id) === -1) {
                fhIds.push(user.custom.id);
                fans.push(fan);

                // THESE HAVE TO BE RE-ADDED FOR UNIQUE REGISTERED USERS IN A SEPARATE LOOP
                // ### BEGIN FIXME ### //
                scFromReg += fan.session_count;
                if (_.has(user, 'p')) {
                    plat.push(user.p);
                }
                if (_.has(user, 'cc') && !_.isEmpty(user.cc) && (user.cc !== 'Unknown')) {
                    cc.push(user.cc);
                }
                if (_.has(user, 'cty') && !_.isEmpty(user.cty) && (user.cty !== 'Unknown')) {
                    cty.push(user.cty);
                }
                if (_.has(user, 'tsd') && _.has(user, 'sc') && (user.sc > 20)) {
                    asd.push(fan.average_session_duration);
                }
                if (fan.push_enabled) {
                    regPushCnt++;
                }
                if (fan.sessions_per_week !== defaultFloatVal) {
                    sess7.push(fan.sessions_per_week);
                }
                // ### END FIXME ### //

            }
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

                        // recalc seconds since last session
                        if (up.last_session_timestamp !== defaultIntVal) {
                            up.seconds_since_last_session = Math.round(Date.now() / 1000 - fans[i].last_session_timestamp);
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
                            up.sessions_per_week = up.session_count * 7.0 / up.days_from_first_to_last_session;
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

                        fans[i] = fan;
                        fans[i].session_count = up.session_count;
                        fans[i].total_session_duration = up.total_session_duration;
                        fans[i].first_session_timestamp = up.first_session_timestamp;
                        fans[i].last_session_timestamp = up.last_session_timestamp;
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
            fans.push(fan);
        }

    });

    /*
    console.log('Total of user records with unique fanhero IDs and register event logged', _.uniq(both).length);
    console.log('Total users with register event logged: ', _.sum(_.map(fans, 'has_registered')));
    */
    var fhidCnt = _.uniq(fhIds).length;
    var uidCnt = _.uniq(_.map(fans, 'uid')).length - 1;
    var _idCnt = _.uniq(_.map(fans, '_id')).length;
    var uniqDevices = _.uniq(_.map(fans, 'device_id')).length - 1;
    var sc = _.sum(_.map(fans, 'session_count'));
    var visByPlat = _.omit(_.countBy(_.map(fans, 'platform')), defaultStrVal);
    var regByPlat = _.countBy(plat);
    var visByCc =  _.omit(_.countBy(_.map(fans, 'country_code')), [defaultStrVal, 'Unknown']);
    var regByCc =  _.countBy(cc);
    var visByCty =  _.omit(_.countBy(_.map(users, 'cty')), [defaultStrVal, 'Unknown']);
    var regByCty =  _.countBy(cty);
    var visByCty150 = _.clone(visByCty);
    for (var k in visByCty) {
        if (visByCty[k] < 150) {
            visByCty150 = _.omit(visByCty150, k);
        }
    }
    var regByCty150 = _.clone(regByCty);
    for (var c in regByCty) {
        if (regByCty[c] < 150) {
            regByCty150 = _.omit(regByCty150, c);
        }
    }
    var provs =  _.omit(_.countBy(_.map(fans, 'email_provider')), defaultStrVal);
    var provs15 = _.clone(provs);
    for (var p in provs) {
        if (provs[p] < 15) {
            provs15 = _.omit(provs15, p);
        }
    }
    var gdr = _.omit(_.countBy(_.map(fans, 'gender')), [defaultStrVal, '-']);
    var ages = _.map(fans, 'age');
    var visPushEn = _.sum(_.map(fans, 'push_enabled'));

    console.log('Total count of unique fanhero IDs: ', fhidCnt);
    console.log('Total entries with fanhero IDs: ', fhIds.length);
    console.log('Total visitor records: ', fans.length);
    console.log('Unique uid entries: ', uidCnt);
    console.log('Unique _id entries: ', _idCnt);
    console.log('Unique device IDs: ', uniqDevices);
    console.log(`Registration rate: ${ Math.round(fhidCnt * 10000.0 / fans.length) / 100.0 } %`);
    console.log('Total session count: ', sc);
    console.log('Total sessions from registered users: ', scFromReg);
    console.log('% of sessions from registered users: ', Math.round(scFromReg * 10000.0 / sc) / 100.0);
    console.log('Average sessions per registered user for more than 5 days: ', scFromReg / fhidCnt);
    console.log('Average sessions per week: ', _.sum(sess7) / sess7.length);
    console.log('all users by platform', visByPlat);
    console.log('registered users by platform', regByPlat);
    console.log('Android registration rate', regByPlat.Android * 100.0 / visByPlat.Android);
    console.log('iOS registration rate', regByPlat.iOS * 100.0 / visByPlat.iOS);
    //console.log('all users by country', visByCc);
    //console.log('registered users by country', regByCc);
    console.log('visitor count from brazil', visByCc.BR, ` (${visByCc.BR * 100.0 / fans.length} %)`);
    console.log('registered fans from brazil', regByCc.BR, ` (${regByCc.BR * 100.0 / fhidCnt} %)`);
    console.log('cities with more than 150 visitors: ', orderedJsonStringify(visByCty150));
    console.log('cities with more than 150 registered users: ', orderedJsonStringify(regByCty150));
    console.log(`visitors from ${Object.keys(visByCty).length} cities`);
    console.log(`registered users from ${Object.keys(regByCty).length} cities`);
    console.log('app version adoption ', orderedJsonStringify(_.omit(_.countBy(_.map(fans, 'app_version')), defaultStrVal)));
    console.log('median session duration for reg. users with 20+ sessions:', stats.median(asd), 'seconds');
    console.log('screen widths:', orderedJsonStringify(_.omit(_.countBy(_.map(fans, 'screen_width')), defaultStrVal)));
    console.log('most common screen width:', stats.mode(_.map(fans, 'screen_width')));
    console.log('email providers with 15+ fans: ', orderedJsonStringify(provs15));
    console.log(`male fans: ${gdr.M} (${Math.round(gdr.M * 10000.0 / (gdr.M + gdr.F)) / 100.0}%)`);
    console.log(`female fans: ${gdr.F} (${Math.round(gdr.F * 10000.0 / (gdr.M + gdr.F)) / 100.0}%)`);
    console.log(`most common fan age: ${stats.mode(ages)}`);
    console.log(`median fan age: ${stats.median(ages)}`);
    console.log(`average fan age: ${stats.mean(ages)}`);
    console.log('fan distribution by astrological sign:', orderedJsonStringify(_.omit(_.countBy(_.map(fans, 'astrological_sign')), [defaultStrVal, 'undefined'])));
    console.log('fan distribution by birth month:', orderedJsonStringify(_.omit(_.countBy(_.map(fans, 'birth_month')), [defaultStrVal, 'undefined', defaultIntVal])));
    console.log('visitor distribution by language:', orderedJsonStringify(_.omit(_.countBy(_.map(fans, 'language')), [defaultStrVal])));
    console.log(`visitors with push notifications enabled: ${visPushEn} (${Math.round(visPushEn * 10000.0 / fans.length) / 100.0} %)`);
    console.log(`registered users with push notifications enabled: ${regPushCnt} (${Math.round(regPushCnt * 10000.0 / fhidCnt) / 100.0} %)`);
    // TODO push notifications statistics by platform (among reg and non-reg users)
}
catch (err) {
    console.error('error while decoding parsed file: ', err);
    process.exit();
}

try {
    fs.writeFileSync(jsonOutputFilePath, JSON.stringify(fans, null, 4));
}
catch (err) {
    console.error('error while writing json output file: ', err);
    process.exit();
}

try {
    fs.writeFileSync(csvOutputFilePath, json2csv({ data: fans}));
}
catch (err) {
    // Errors are thrown for bad options, or if the data is empty and no fields are provided.
    // Be sure to provide fields if it is possible that your data array will be empty.
    console.error('error while writing csv output file', err);
}

var rstream = fs.createReadStream(csvOutputFilePath);
var wstream = fs.createWriteStream(sqlOutputFilePath);
rstream.pipe(csv2sql).pipe(wstream); // mysql -u root -p < dump.sql
