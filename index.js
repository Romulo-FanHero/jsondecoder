var _ = require('lodash');

/*
fs.readFile('temp/temp.json', 'utf8', function(err, data) {
    if (err) throw err;
    console.log(data);
});
*/

try {
    //var data = fs.readFileSync('temp/temp.json', 'utf8');
    var users = JSON.parse(require('fs').readFileSync('temp/temp.json', 'utf8'));
    var cnt = 0;
    users.forEach(function(user) {
        //console.log(JSON.stringify(user, null, 2));
        if (
            _.has(user, 'custom.status') && user.custom.status === 'ENABLED'
        ) {
            cnt++;
        }
    });
    //console.log(JSON.stringify(users.slice(0, 10), null, 2));
    //console.log(JSON.stringify(users, null, 2));
    //console.log(users.length);
}
catch (err) {
    console.error(err);
}

console.log('first 10 fans', JSON.stringify(users.slice(0,10), null, 2));
console.log('COUNT', cnt);

/*
var _id = [];
data.match(/"_id" : "(.*)",/g).forEach(function(match) {
    try {
        _id.push(match.split(`"_id" : "`)[1].split(`",`)[0]);
    }
    catch (e) {
    }
});
console.log('TOTAL IDS FOUND: ', _id.length);

var sc = [];
data.match(/"sc" : (.*),/g).forEach(function(match) {
    try {
        var val = parseInt(match.split(`"sc" : `)[1].split(`,`)[0], 10);
        sc.push(_.isFinite(val) ? val : 0);
    }
    catch (e) {
        sc.push(0);
    }
});
console.log('TOTAL SCs FOUND: ', sc.length);

console.log('SC COUNT: ', data.match(/"sc" /g).length);

console.log('FILE LENGTH: ', data.length);
*/
