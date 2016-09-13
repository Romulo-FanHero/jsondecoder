var _ = require('lodash');
var fs = require('fs');
var horoscope = require('horoscope');
var toAge = require ('to-age');
var ev = require("email-validator");

const dumpFilePath = 'temp/temp.json';
const fixedFilePath = 'temp/fixed.json';
const outputFilePath = 'temp/treated.json';

const defaultIntVal = null; // -1
const defaultFloatVal = null; // NaN
const defaultStrVal = null; // 'n/a'

try {
    // check if fixed file already exist, thowing an exception if it doesn't
    fs.accessSync(fixedFilePath, fs.F_OK);
}
catch (e) {
    // try to create fixed file
    try {
        fs.writeFileSync(fixedFilePath,
            fs.readFileSync(dumpFilePath, 'utf8')
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
    var users = JSON.parse(fs.readFileSync('temp/fixed.json', 'utf8'));
}
catch (err) {
    console.error('error while parsing fixed file: ', err);
    process.exit();
}

try {
    // try to decode user array into fan array
    var fans = [];
    users.forEach(function(user) {

        var fan = {};
        fan.session_count = _.has(user, 'sc') ? user.sc : 0;
        fan.first_session_timestamp = _.has(user, 'fs') ? user.fs : defaultIntVal;
        fan.last_session_timestamp = _.has(user, 'ls') ? user.ls : defaultIntVal;
        fan.seconds_since_first_session = _.has(user, 'fs') ? Math.round(Date.now() / 1000 - user.fs) : defaultIntVal;
        fan.seconds_since_last_session = _.has(user, 'ls') ? Math.round(Date.now() / 1000 - user.ls) : defaultIntVal;
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
        fan.avatar_available = _.has(user, 'hasInfo') && user.hasInfo && _.has(user, 'picture'); // && isValidUrl(user.picture)
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
                if ( Object.prototype.toString.call(date) === "[object Date]" ) {
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

        // Event counters
        fan.msg_count = _.has(user, 'msgs') ? user.msgs.length : 0;
        fan.event_count = _.has(user, 'pe') ? user.pe.length : 0;
        fan.alert_count = null; // FIXME
        fan.authenticate_count = null; // FIXME
        fan.authenticate_error_count = null; // FIXME
        fan.edit_profile_count = null; // FIXME
        fan.edit_profile_error_count = null; // FIXME
        fan.forgot_password_count = null; // FIXME
        fan.forgot_password_error_count = null; // FIXME
        fan.like_count = null; // FIXME
        fan.livestream_closed_count = null; // FIXME
        fan.livestream_opened_count = null; // FIXME
        fan.push_opened_count = null; // FIXME
        fan.play_video_count = null; // FIXME
        fan.post_comment_count = null; // FIXME
        fan.post_details_count = null; // FIXME
        fan.post_like_count = null; // FIXME
        fan.register_count = null; // FIXME
        fan.register_error_count = null; // FIXME
        fan.sign_out_count = null; // FIXME
        fan.stream_fetch_error_count = null; // FIXME
        fan.unlike_count = null; // FIXME
        fan.zoom_close_count = null; // FIXME

        // booleans from event counters
        fan.has_authenticated = null; // FIXME
        fan.has_edited_profile = null; // FIXME
        fan.has_forgot_password = null; // FIXME
        fan.has_liked = null; // FIXME
        fan.has_livestreamed = null; // FIXME
        fan.has_played_video = null; // FIXME
        fan.has_commented = null; // FIXME
        fan.has_registered = null; // FIXME
        fan.has_signed_out = null; // FIXME

        fans.push(fan);

    });
}
catch (err) {
    console.error('error while decoding parsed file: ', err);
    process.exit();
}

try {
    fs.writeFileSync(outputFilePath, JSON.stringify(fans, null, 4));
}
catch (err) {
    console.error('error while writing output file: ', err);
}
