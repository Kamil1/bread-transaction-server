var firebase = require('firebase');
var bodyParser = require('body-parser');
var express = require('express');
var pg = require('pg');
var uuid = require('uuid');

var app = express();
var port = process.env.PORT || 8081;
var jsonParser = bodyParser.json();

firebase.initializeApp({
  serviceAccount: 'conf/firebase_service_account_credentials.json',
  databaseURL: 'https://bread-e6858.firebaseio.com'
});

app.use(bodyParser.json());

app.get('/', function(request, response) {
  response.writeHead(200, {'Content-Type': 'text/plain'});
  response.end('A-Ok');
});

app.listen(port, function() {
    console.log('App is running on http://localhost:%s', port);
});

app.post('/create_transaction', jsonParser, function(request, response) {
    if (!request.body) return response.sendStatus(400);
    var transactionID = uuid.v4();
    var userID        = "";
    var clientID      = request.body.client_id;
    var itemID        = request.body.item;
    var quantity      = request.body.quantity;
    var bread         = request.body.bread;
    var token         = request.body.user_token;

    firebase.auth().verifyIdToken(token).then(function(decodedToken) {
      userID = decodedToken.uid;
    }).catch (function (error) {
        console.log(error);
        return response.status(401).send("Unauthorized");
    });

  var queryText = 'INSERT INTO public.pending_transactions VALUES ($1, $2, $3, $4, $5, $6)';

  pg.connect(process.env.DATABASE_URL, function(err, client, done) {
    client.query(queryText, [transactionID, userID, clientID, itemID, quantity, bread], function (err, result) {
      if (err) response.sendStatus(500);
      else {
          response.status(200).json({transaction_id : transactionID});
      }
    });
  });

});