// Get the JSON file
var fs = require('fs');
var stream = fs.createReadStream('temp/temp.json', {flags: 'r', encoding: 'utf-8'});
var buf = '';
var count = 0;

stream.on('data', function(chunk) {
    buf += chunk.toString(); // when data is read, stash it in a string buffer
    process(buf); // process the buffer
});
stream.on('error', function(err) {
    console.log(err);
});
stream.on('end', function() {
    console.log("Count: " + count);
});

function process() {
    var posStart = buf.indexOf(`{`);
    var posEnd = buf.indexOf(`}`);

    while (posStart >= 0 || posEnd >= 0) { // keep going until the start or end of the json object in the string
        // IF the start bracket is before the end, skip to the start
        if((posStart < posEnd || posEnd < 0) && posStart >= 0){
            buf = buf.slice(posStart);
        }
        if(posStart === 0 && posEnd >= 0){ // IF the end bracket is next
            processObjectString(buf.slice(0, posEnd+1)); // Process the complete object string
            buf = buf.slice(posEnd+1); // Remove the processed string from the buffer
        }else if(posStart < 0 || posEnd < 0){ // Return to get a new chunk
            return;
        }
        // Update the positions
        posStart = buf.indexOf('{');
        posEnd = buf.indexOf('}');
    }
}

function processObjectString(objectString) {
    count++;
    try {
        var obj = JSON.parse(objectString); // parse the JSON
        console.log(JSON.stringify(obj, null, 2)); // Print object ID (works)
    }
    catch(err) {
    }

}
