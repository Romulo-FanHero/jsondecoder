require('events').EventEmitter.prototype._maxListeners = 0;

var prompt = require('prompt');

var _ = require('lodash');
var sleep = require('system-sleep');

var fs = require('fs'),
    readline = require('readline');

//const inputFilePath = 'temp/mod_inter.json';
const inputFilePath = 'temp/full.json';
//const inputFilePath = 'temp/intermediate.json';

var rd = readline.createInterface({
    input: fs.createReadStream(inputFilePath),
    output: process.stdout,
    terminal: false
});

var buff = '';
var users = [];
var cnt = 0;
rd.on('line', function(line) {
    if (cnt === 36327) {
        //console.log('buffer:');
        //console.log(buff);
        //console.log();
        console.log('Sleeping...');
        sleep(86400000);
    }
    try {
        //var pars = JSON.parse(buff + line.split('},').join('}'));
        //cnt++;
        //console.log(cnt);
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
