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

var firebaseDB = firebase.database();

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
    var userIDTest    = request.body.user_id;
    var clientID      = request.body.client_id;
    var item          = request.body.item;
    var quantity      = request.body.quantity;
    var bread         = request.body.bread;
    // var token         = request.idToken;
    //
    // firebase.auth().verifyIdToken(token).then(function(decodedToken) {
    //   userID = decodedToken.uid;
    // }).catch (function (error) {
    //   return response.sendStatus(400);
    // });

  var queryText = 'INSERT INTO public.pending_transactions VALUES ($1, $2, $3, $4, $5, $6)';

  pg.connect(process.env.DATABASE_URL, function(err, client, done) {
    client.query(queryText, [transactionID, userIDTest, clientID, item, quantity, bread], function (err, result) {
      if (err) console.log(err);
      else {
          monitorTransaction(userID);
        // return transaction ID to user (who will record it themselves in Firebase) and place monitor on uid's "transaction history" for 3 mins
        // if it appears in "approved transactions", take steps to make it go through
        // if it appears in "declined transactions", take steps for it to fail
      }
    });
  });

  response.sendStatus(200);
});

function monitorTransaction(uid) {
    var ref = firebaseDB.ref("users/" + uid + "/transactions/pending_transactions");
    console.log("monitoring user response");
    function transactionCallback(snapshot, prevChildKey) {
        var transactionID = snapshot.val();
        executeTransaction(transactionID, uid);
    }
    ref.once("child_added", transactionCallback);
}

function executeTransaction(transactionID, uid) {
    console.log("CALLBACK CALLED!");
    var ref = firebaseDB.ref("users/" + uid + "/pantry/balance");
    pg.connect(process.env.DATA_URL, function(err, client, done) {
        client.query('SELECT * FROM pending_transactions WHERE transaction_id = $1', [transactionID], function (err, result) {
            if (err) throw err;
            console.log(result.rows[0]);
        })
    });
}