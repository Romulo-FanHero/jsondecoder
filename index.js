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

// data = data.substr(0, 10000);

console.log(data.length);

var matches = data.match(/        "_id" : "(.*)",/g);
matches.forEach(function(match) {
    var id = match.split(`        "_id" : "`)[1].split(`",`)[0];
    console.log(id);
});
console.log(matches.length);
