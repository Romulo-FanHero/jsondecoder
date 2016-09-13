var _ = require('lodash');
var fs = require('fs');

//var originalFileData = fs.readFileSync('temp/temp.json', 'utf8');
//var firstLineRemoved = originalFileData.split(`/* 1 */`).join(``);
//var objCallRemoved = (firstLineRemoved.split(`ObjectId("`).join(`"`)).split(`")`).join(`"`);
//fs.writeFileSync('temp/temp2.json', objCallRemoved);

//fs.writeFileSync('temp/fixed.json',((((fs.readFileSync('temp/temp.json', 'utf8')).split(`/* 1 */`).join(``)).split(`ObjectId("`).join(`"`)).split(`")`).join(`"`)));

//fs.writeFileSync('temp/fixed.json', (((
//                fs.readFileSync('temp/temp.json', 'utf8')
//            ).split(`/* 1 */`).join(``)
//        ).split(`ObjectIdx"`).join(`"`)
//    ).split(`"x`).join(`"`)
//);

fs.writeFileSync('temp/fixed.json',
    fs.readFileSync('temp/temp.json', 'utf8')
    .split(`/* 1 */`).join(``)
    .split(`ObjectId("`).join(`"`)
    .split(`")`).join(`"`)
);

try {
    var users = JSON.parse(fs.readFileSync('temp/fixed.json', 'utf8'));
    var cnt = 0;
    users.forEach(function(user) {
        if (
            _.has(user, 'custom.status') && user.custom.status === 'ENABLED'
        ) {
            cnt++;
        }
    });
    console.log('first 10 fans', JSON.stringify(users.slice(0, 10), null, 2));
    console.log('COUNT', cnt);
}
catch (err) {
    console.error(err);
}
