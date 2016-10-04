require('events').EventEmitter.prototype._maxListeners = 0;

var _ = require('lodash');
var sleep = require('system-sleep');

var fs = require('fs'),
    readline = require('readline');

var buff = '';
var users = [];

readline.createInterface({
    input: fs.createReadStream('temp/full.json'),
    output: process.stdout,
    terminal: false
}).on('line', function(line) {
    try {
        users.push(JSON.parse(buff + line.split('},').join('}')));
        console.log(users.length);
        buff = '';
    }
    catch (err) {
        buff += line.split(`ISODate("`).join(`"`)
                    .split(`Date(`).join(` `)
                    .split(`ObjectId("`).join(`"`)
                    .split(`")`).join(`"`)
                    .split(`)`).join(` `); // << danger
        if ((buff.indexOf(',') !== -1 && ((buff.indexOf('{') === -1) || (buff.indexOf(`"`) === `"`))) || (buff.indexOf(`/*`) !== -1) || (buff.indexOf(`*/`) !== -1) || (line === `[`) || (line === ` [`) || (line === `[ `)) {
            buff = '';
        }
    }
});
