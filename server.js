var firebase = require('firebase');
var bodyParser = require('body-parser');
var express = require('express');
var pg = require('pg');
var uuid = require('uuid');
var url = require('url');

var pgParams = url.parse(process.env.DATABASE_URL);
var pgAuth   = pgParams.auth.split(':');

var poolConfig = {
    user: pgAuth[0],
    password: pgAuth[1],
    host: pgParams.hostname,
    port: pgParams.port,
    database: pgParams.pathname.split('/')[1],
    max: 10
};

var pool = new pg.Pool(poolConfig);

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
    if (!request.body) return response.status(400).json({error: "Bad Request"});
    var transactionID = uuid.v4();
    var clientID      = request.body.client_id;
    var itemID        = request.body.item;
    var quantity      = request.body.quantity;
    var bread         = request.body.bread;
    var token         = request.body.user_token;

    function setupTransaction(userID) {
        pool.connect(function (err, client, done) {
            var countPendingTransactions = "SELECT COUNT(*) AS pending_transactions FROM public.pending_transaction WHERE user_id = $1 AND created_datetime + 90 >= EXTRACT(EPOCH FROM NOW())";
            client.query(countPendingTransactions, [userID], function (err, result) {
                done();

                if (err) {
                    console.log("select");
                    response.status(500).json({error: "Internal Server Error"});
                    return;
                }
                if (result.rows[0].pending_transactions > 3) {
                    response.status(429).json({error: "Too Many Requests"});
                    return;
                }
                createTransaction(userID)
            });
        });
    }

    function createTransaction(userID) {
        console.log("creating transaction");
        pool.connect(function (err, client, done) {
            var insertPendingTransaction = 'INSERT INTO public.transaction VALUES ($1, $2, $3, $4, $5, $6)';
            client.query(insertPendingTransaction, [transactionID, userID, clientID, itemID, quantity, bread], function (err, result) {
                done();

                if (err) {
                    console.log("insert: " + err);
                    response.status(500).json({error: "Internal Server Error"});
                    return;
                }
                createPendingTransaction(userID);
            });
        });
    }

    function createPendingTransaction(userID) {
        pool.connect(function (err, client, done) {
            var insertPendingTransaction = 'INSERT INTO public.pending_transaction VALUES ($1, $2, $3, $4, $5, $6)';
            client.query(insertPendingTransaction, [transactionID, userID, clientID, itemID, quantity, bread], function (err, result) {
                done();

                if (err) {
                    console.log("insert: " + err);
                    response.status(500).json({error: "Internal Server Error"});
                    return;
                }
                response.status(200).json({transaction_id: transactionID});
            });
        });
    }

    firebase.auth().verifyIdToken(token).then(function(decodedToken) {
        setupTransaction(decodedToken.uid);
    }).catch(function(error) {
        console.log(error);
        response.status(401).json({error: "Unauthorized"});
    });

});

app.post('/execute_transaction', jsonParser, function(request, response) {
    if (!request.body) {
        response.status(400).json({error: "Bad Request"});
        return;
    }
    var transactionID = request.body.transaction_id;
    var token         = request.body.user_token;

    function invoiceTransaction() {
        pool.connect(function(err, client, done) {
            var insertTransaction = 'INSERT INTO public.fulfilled_transaction VALUES ($1)';
            client.query(insertTransaction, [transactionID], function(err, result) {
                done();

                if (err) {
                    console.log("Error recording transaction");
                    response.status(500).json({error: "Internal Server Error"});
                    return;
                }
                deletePendingTransaction();
            })
        })
    }

    function deletePendingTransaction() {
        pool.connect(function(err, client, done) {
            var deletePendingTransaction = 'DELETE FROM public.pending_transaction WHERE transaction_id = $1';
            client.query(deletePendingTransaction, [transactionID], function(err, result) {
                done();

                if (err) {
                    console.log("Error deleting pending transaction");
                    response.status(500).json({error: "Internal Server Error"});
                    return;
                }
                console.log("Transaction Successfully Completed");
                response.status(200).json({result: "Transaction Successfully Completed"});
            })
        })
    }

    function executeTransaction(tokenUserID) {
        pool.connect(function(err, client, done) {
            var selectTransaction = 'SELECT * FROM public.pending_transaction WHERE transaction_id = $1';
            client.query(selectTransaction, [transactionID], function(err, result) {
                done();

                if (err) {
                    response.status(500).json({error: "Internal Server Error"});
                    return;
                }
                var row = result.rows[0];

                var now = (new Date).getTime() / 1000;
                var created_datetime = row.created_datetime;

                if (now - created_datetime > 90) {
                    response.status(410).json({error: "Gone: Transaction Expired"});
                    return;
                }

                var clientID = row.client_id;
                var item = row.item_id;
                var quantity = row.quantity;
                var bread = row.bread;
                var userID = row.user_id;

                if (userID != tokenUserID) {
                    response.status(401).json({error: "Unauthorized"});
                    return;
                }

                var beginningBalance = 0;
                var userPantry = firebase.database().ref('users/' + userID + '/pantry/bread_balance');
                userPantry.transaction(function(currentBalance) {
                    console.log("The current balance is: " + currentBalance);
                    beginningBalance = currentBalance;
                    if (currentBalance >= bread) {
                        return currentBalance - bread;
                    } else {
                        return currentBalance;
                    }
                }, function(error, committed, snapshot) {
                    if (error) {
                        console.log("Error accessing user pantry");
                        response.status(500).json({error: "Internal Server Error"});
                    } else if (!committed) {
                        // TODO: Provide result codes as seperate field in response json for easier parsing
                        console.log("Pantry transaction not committed");
                        response.status(200).json({result: "Insufficient Funds"});
                    } else {

                        if (beginningBalance == snapshot.val()) {
                            console.log("Insufficient funds");
                            response.status(200).json({result: "Insufficient Funds"});
                            return;
                        }

                        var userItem = firebase.database().ref('users/' + userID + '/clients/' + clientID + '/' + item);
                        console.log("Crediting user items");
                        userItem.transaction(function(currentItem) {
                            if (currentItem === null) return quantity;
                            return currentItem + quantity;
                        }, function(error) {
                            if (error) {
                                console.log("error creditting item to user account");
                                response.status(500).json({error: "Internal Server Error"});
                            } else {
                                console.log("Invoicing transaction");
                                invoiceTransaction();
                            }
                        })
                    }
                });
            })
        })
    }
   
    firebase.auth().verifyIdToken(token).then(function(decodedToken) {
        executeTransaction(decodedToken.uid);
    }).catch(function(error) {
        console.log(error);
        response.status(401).json({error: "Unauthorized"});
    });

});
