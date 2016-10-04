require('events').EventEmitter.prototype._maxListeners = 0;

var _ = require('lodash');
var sleep = require('system-sleep');

var fs = require('fs'),
    readline = require('readline');

const inputFilePath = 'temp/input.json';
const intermediateFilePath = 'temp/intermediate.json';

fs.writeFileSync(intermediateFilePath, '[');

var buff = '';
var cnt = 0;
readline.createInterface({
    input: fs.createReadStream(inputFilePath),
    output: process.stdout,
    terminal: false
}).on('line', function(line) {
    try {
        fs.appendFileSync(intermediateFilePath, JSON.stringify(JSON.parse(buff + line.split('},').join('}')), null, 2) + ',\n', encoding = 'utf8');
        cnt++;
        console.log(cnt);
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
