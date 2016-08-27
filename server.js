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
            var countPendingTransactions = "SELECT COUNT(*) AS pending_transactions FROM public.pending_transactions WHERE user_id = $1 AND created_datetime + 90 >= EXTRACT(EPOCH FROM NOW())";
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
                createPendingTransaction(userID)
            });
        });
    }

    function createPendingTransaction(userID) {
        console.log("creating pending transaction");
        pool.connect(function (err, client, done) {
            var insertPendingTransaction = 'INSERT INTO public.pending_transactions VALUES ($1, $2, $3, $4, $5, $6)';
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
            var insertTransaction = 'INSERT INTO public.transactions SELECT transaction_id, user_id, client_id, item_id, quantity, bread FROM pending_transactions WHERE transaction_id = $1';
            client.query(insertTransaction, [transactionID], function(err, result) {
                done();

                if (err) {
                    response.status(500).json({error: "Internal Server Error"});
                    return;
                }
                deletePendingTransaction();
            })
        })
    }

    function deletePendingTransaction() {
        pool.connect(function(err, client, done) {
            var deletePendingTransaction = 'DELETE FROM public.transaction WHERE transaction_id = $1';
            client.query(deletePendingTransaction, [transactionID], function(err, result) {
                done();

                if (err) {
                    response.status(500).json({error: "Internal Server Error"});
                    return;
                }
            })
        })
    }

    function executeTransaction(tokenUserID) {
        pool.connect(function(err, client, done) {
            var selectTransaction = 'SELECT * FROM public.pending_transactions WHERE transaction_id = $1';
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

                var breadBalancePath = 'users/' + userID + '/pantry/bread_balance';
                console.log(breadBalancePath);
                var userPantry = firebase.database().ref(breadBalancePath);
                userPantry.transaction(function(currentBalance) {
                    console.log("The current balance is: " + currentBalance);
                    if (currentBalance >= bread) {
                        return currentBalance - bread;
                    } else {
                        return currentBalance;
                    }
                }, function(error, committed) {
                    if (error) {
                        response.status(500).json({error: "Internal Server Error"});
                    } else if (!committed) {
                        // TODO: Provide result codes as seperate field in response json for easier parsing
                        response.status(200).json({result: "Insufficient Funds"});
                    } else {
                        var userItem = firebase.database().ref('users/' + userID + '/clients/' + clientID + '/' + item);
                        userItem.transaction(function(currentItem) {
                            if (currentItem === null) return quantity;
                            return currentItem + quantity;
                        }, function(error) {
                            if (error) {
                                response.status(500).json({error: "Internal Server Error"});
                            } else {
                                invoiceTransaction();
                                response.status(200).json({result: "Transaction Completed Successfully"});
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
