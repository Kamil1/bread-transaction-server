var express = require("express");
var app = express();
var port = process.env.PORT || 8081;

app.get('/', function(request, response) {
	response.writeHead(200, {'Content-Type': 'text/plain'});
	response.end('A-Ok');
});

app.listen(port, function() {
    console.log('App is running on http://localhost:' + port);
});