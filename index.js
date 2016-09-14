var _ = require('lodash');
var fs = require('fs');
var horoscope = require('horoscope');
var toAge = require ('to-age');
var ev = require('email-validator');
var json2csv = require('json2csv');

const inputFilePath = 'temp/input.json';
const intermediateFilePath = 'temp/intermediate.json';
const jsonOutputFilePath = 'temp/output.json';
const csvOutputFilePath = 'temp/output.csv';

const defaultIntVal = null; // -1
const defaultFloatVal = null; // NaN
const defaultStrVal = null; // 'n/a'

function countEvent(user, eventName) {
    if (!_.has(user, 'pe')) {
        return 0;
    }
    //console.log(user.pe);
    return _.reduce(user.pe, function(n, val) {
        return n + (val === eventName);
    }, 0);
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

    var both = [];
    var sess7 = [];
    var plat = [];
    var cc = [];
    var cty = [];
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
        fan.days_from_first_to_last_session = _.has(user, 'fs') && _.has(user, 'ls') ? Math.floor((fan.last_session_timestamp - fan.first_session_timestamp)/86400) : defaultIntVal;
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
        fan.total_session_duration = _.has(user, 'tsd') ? Math.round(user.tsd) : defaultIntVal;
        fan.average_session_duration = _.has(user, 'tsd') && _.has(user, 'sc') && (user.sc > 0) ? Math.round(user.tsd / user.sc) : defaultIntVal;
        fan.location = _.has(user, 'custom.location') ? user.custom.location : defaultStrVal;
        fan.fanheroid = _.has(user, 'custom.id') ? user.custom.id : defaultStrVal;
        fan._id = _.has(user, '_id') ? user._id : defaultStrVal;
        fan.uid = _.has(user, 'uid') ? user.uid : defaultStrVal;
        fan.push_enabled = (_.has(user, 'tkip') && user.tkip) || (_.has(user, 'tkap') && user.tkap);
        fan.device_resolution = _.has(user, 'r') ? user.r : defaultStrVal;
        fan.device_density = _.has(user, 'dnst') ? user.dnst : defaultStrVal;
        fan.language = _.has(user, 'la') ? user.dnst : defaultStrVal;
        fan.avatar_available = _.has(user, 'hasInfo') && user.hasInfo && _.has(user, 'picture'); // && isValidUrl(user.picture) FIXME hasInfo might mean more than this
        fan.avatar_url = _.has(user, 'picture') ? user.picture : defaultStrVal; // "https://api.fanhero.net/user/${fan.fanheroid}/avatar/thumb-100"
        fan.email = _.has(user, 'email') ? user.email : defaultStrVal;
        fan.email_valid = ev.validate(fan.email);
        fan.email_provider = fan.email_valid ? fan.email.split('@').pop().split('.').shift() : defaultStrVal;
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

        // ### DANGER ###
        fan.name = _.has(user, 'name') ? user.name : defaultStrVal;
        fan.username = _.has(user, 'username') ? user.username : defaultStrVal;

        // queue completed fan
        fans.push(fan);

        if (_.has(user, 'custom.id') && !_.isEmpty(user.custom.id)) {
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
            /*
            if (fan.has_registered) {
                both.push(user.custom.id);
            }
            */
        }
        if (fan.sessions_per_week !== defaultFloatVal) {
            sess7.push(fan.sessions_per_week);
        }

    });
    /*
    console.log('Total of user records with unique fanhero IDs and register event logged', _.uniq(both).length);
    console.log('Total users with register event logged: ', _.sum(_.map(fans, 'has_registered')));
    */
    var fhidCnt = _.uniq(_.map(fans, 'fanheroid')).length - 1;
    var sc = _.sum(_.map(fans, 'session_count'));
    var visByPlat = _.omit(_.countBy(_.map(fans, 'platform')), defaultStrVal);
    var regByPlat = _.countBy(plat);
    var visByCc =  _.omit(_.countBy(_.map(fans, 'country_code')), [defaultStrVal, 'Unknown']);
    var regByCc =  _.countBy(cc);
    var visByCty =  _.omit(_.countBy(_.map(fans, 'cty')), [defaultStrVal, 'Unknown']);
    var regByCty =  _.countBy(cty);
    console.log('Total count of unique fanhero IDs: ', fhidCnt);
    console.log('Total visitor records: ', fans.length);
    console.log(`Registration rate: ${ Math.round(fhidCnt * 10000.0 / fans.length) / 100.0 } %`);
    console.log('Total session count: ', sc);
    console.log('Total sessions from registered users: ', scFromReg);
    console.log('% of sessions from registered users: ', Math.round(scFromReg * 10000.0 / sc) / 100.0);
    console.log('Average sessions per registered user for more than 5 days: ', scFromReg/fhidCnt);
    console.log('Average sessions per week: ', _.sum(sess7)/sess7.length);
    console.log('all users by platform', visByPlat);
    console.log('registered users by platform', regByPlat);
    console.log('Android registration rate', regByPlat.Android * 100.0 / visByPlat.Android);
    console.log('iOS registration rate', regByPlat.iOS * 100.0 / visByPlat.iOS);
    //console.log('all users by country', visByCc);
    //console.log('registered users by country', regByCc);
    console.log('visitor count from brazil', visByCc.BR, ` (${visByCc.BR * 100.0 / fans.length} %)`);
    console.log('registered fans from brazil', regByCc.BR, ` (${regByCc.BR * 100.0 / fhidCnt} %)`);
    console.log('visitors by city', visByCty);
    console.log('registered by city', regByCty);
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
