var fs = require('fs'),
  JSONStream = require('JSONStream'),
  es = require('event-stream');

var getStream = function () {
    var jsonData = 'temp.json',
        stream = fs.createReadStream(jsonData, {encoding: 'utf8'}),
        parser = JSONStream.parse('*');
        return stream.pipe(parser);
};

 getStream()
  .pipe(es.mapSync(function (data) {
    console.log(data);
  }));
