var fs = require('fs');

/*
fs.readFile('temp/temp.json', 'utf8', function(err, data) {
    if (err) throw err;
    console.log(data);
});
*/

try {
    var data = fs.readFileSync('temp/temp.json', 'utf8');
}
catch (err) {
    console.error(err);
}

var ids = [];
var matches = data.match(/        "_id" : "(.*)",/g);
matches.forEach(function(match) {
    var id = match.split(`        "_id" : "`)[1].split(`",`)[0];
    console.log(id);
    ids.push(id);
});
console.log('TOTAL IDS FOUND: ', ids.length);
console.log('FILE LENGTH: ', data.length);
