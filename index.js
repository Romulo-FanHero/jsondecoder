var _ = require('lodash');
var fs = require('fs');

const dumpFilePath = 'temp/temp.json';
const fixedFilePath = 'temp/fixed.json';

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

var cnt = 0;
users.forEach(function(user) {
    if (_.has(user, 'custom.status')) {
        cnt++;
    }
});
console.log('First 10 fans', JSON.stringify(users.slice(0, 10), null, 2));
console.log('COUNT', cnt);
