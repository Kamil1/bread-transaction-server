var firebase = require('firebase');
var bodyParser = require('body-parser');
var pg = require('pg');
var uuid = require('uuid');
var url = require('url');
var makeRequest = require('request');
var express = require('express');
var jsSHA = require("jssha");

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

var POST_SERVER = "https://bread-post-server.herokuapp.com/";

var begin = function(client, done, query) {
    client.query('BEGIN', function(err) {

        if (err) {
            rollback(client, done);
            return false;
        }

        return query(client, done);
    })
};

var commit = function(client, done) {
    client.query('COMMIT', done);
};

var rollback = function(client, done) {
    client.query('ROLLBACK', function(err) {
        done(err);
    })
};

function movePendingTransaction(transactionID, table, response, callback) {

    function checkTransaction(client, done) {
        var checkTransaction = 'SELECT COUNT(*) AS pending_transactions FROM public.pending_transaction WHERE transaction_id = $1';
        client.query(checkTransaction, [transactionID], function (err, result) {

            if (err) {
                console.log("insert: " + err);
                response.status(500).json({error: "Internal Server Error"});
                return;
            }

            if (result.rows[0].pending_transactions != 1) {
                console.log("Actual count of pending transactions: " + result.rows[0].pending_transactions);
                // TODO: probably expired -- maybe change message to explain it? Would require querying expired transactions table...
                response.status(404).json({error: "Not Found"});
                return;
            }

            transferTransaction(client, done);
        });
    }

    function transferTransaction(client, done) {
        var transferTransaction = "INSERT INTO public.transaction SELECT transaction_id, user_id, client_id, item_id, quantity, bread, TO_TIMESTAMP(created_datetime) AT TIME ZONE 'UTC' AS created_datetime FROM public.pending_transaction WHERE transaction_id = $1";
        client.query(transferTransaction, [transactionID], function (err, result) {

            if (err) {
                console.log("insert: " + err);
                rollback(client, done);
                response.status(500).json({error: "Internal Server Error"});
                return;
            }

            deletePendingTransaction(client, done);
        });
    }

    function deletePendingTransaction(client, done) {
        var deletePendingTransaction = 'DELETE FROM public.pending_transaction WHERE transaction_id = $1';
        client.query(deletePendingTransaction, [transactionID], function (err, result) {

            if (err) {
                console.log("Error deleting pending transaction");
                rollback(client, done);
                response.status(500).json({error: "Internal Server Error"});
                return;
            }

            console.log("Deleted pending transaction");
            categorizeTransaction(client, done);
        })
    }

    function categorizeTransaction(client, done) {
        var categorizeTransaction = 'INSERT INTO public.' + table + ' VALUES ($1)';
        client.query(categorizeTransaction, [transactionID], function(err, result) {

            if (err) {
                console.log("Error recording transaction");
                rollback(client, done);
                response.status(500).json({error: "Internal Server Error"});
                return;
            }

            commit(client, done);
            console.log("Transaction Successfully Completed");
            callback();
        })
    }

    pool.connect(function(err, client, done) {
        begin(client, done, checkTransaction);
    })

}

function expirePendingTransactions() {
    //TODO: add to expired_transaction table

    function moveExpiredTransactions(client, done) {
        var moveExpiredTransactions = "INSERT INTO public.transaction SELECT transaction_id, user_id, client_id, item_id, quantity, bread, TO_TIMESTAMP(created_datetime) AT TIME ZONE 'UTC' AS created_datetime FROM public.pending_transaction WHERE EXTRACT(EPOCH FROM NOW()) - created_datetime > 90 RETURNING transaction_id";
        client.query(moveExpiredTransactions, function(err, result) {

            if (err) {
                console.log("insert" + err);
                rollback(client, done);
                return;
            }

            var transactionIds = [];

            Object.keys(result).map(function(value, index) {
                transactionIds.push(value);
            });

            console.log("Moved expired pending transactions into transaction table");

            deleteExpiredTransactions(client, done, transactionIds);
        })
    }

    function deleteExpiredTransactions(client, done, transactionIds) {
        var transactionMapping = [];
        transactionIds.forEach(function(value, index) {
            transactionMapping.push("$" + index);
        });
        var deleteExpiredTransactions = "DELETE FROM public.pending_transaction WHERE transaction_id IN (" + transactionIds.join(",") + ")";
        client.query(deleteExpiredTransactions, transactionMapping, function(err, result) {

            if (err) {
                console.log("Error deleting expired pending transactions");
                rollback(client, done);
                return;
            }

            commit(client, done);
            console.log("Successfully expired pending transactions");
        })
    }

    pool.connect(function(err, client, done) {
        begin(client, done, moveExpiredTransactions);
    })
}

function saveToFirebase(transactionID, clientID, itemID, quantity, bread, userID, timestamp, callback) {
    var userRef = firebaseDB.ref("users/" + userID + "/transactions/");
    var userTransaction = {
        client_id: clientID,
        item_id: itemID,
        "quantity": quantity,
        "bread": bread,
        "timestamp": timestamp
    };

    var transactionRef = firebaseDB.ref("transactions");
    var transaction = {
        user_id: userID,
        client_id: clientID,
        item_id: itemID,
        "quantity": quantity,
        "bread": bread,
        "timestamp": timestamp
    };

    userRef.child(transactionID).set(userTransaction, function() {
        transactionRef.child(transactionID).set(transaction, function() {
            var options = {
                body: {transaction_id: transactionID},
                json: true,
                url: POST_SERVER + "share_transaction"
            };
            makeRequest.post(options, function(error, res, body) {
                if (error) {
                    response.status(500).json({error: "Internal Server Error"});
                    return;
                }
                callback();
            })
        });
    });
}

setInterval(expirePendingTransactions, 300000);

firebase.initializeApp({
    serviceAccount: 'conf/firebase_service_account_credentials.json',
    databaseURL: 'https://bread-e6858.firebaseio.com'
});

var firebaseDB = firebase.database();

app.use(bodyParser.json());

app.listen(port, function() {
    console.log('App is running on http://localhost:%s', port);
});

app.get('/', function(request, response) {
  response.status(200).json({result: 'A-Ok'});
});

app.post('/create_transaction', jsonParser, function(request, response) {
    if (!request.body) {
        response.status(400).json({error: "Bad Request"});
        return;
    }

    var transactionID = uuid.v4();
    var clientID      = request.body.client_id;
    var itemID        = request.body.item_id;
    var quantity      = request.body.quantity;
    var bread         = request.body.bread;
    var token         = request.body.user_token;

    function createTransaction(userID) {

        function setupTransaction(client, done) {
            var countPendingTransactions = "SELECT COUNT(*) AS pending_transactions FROM public.pending_transaction WHERE user_id = $1 AND created_datetime + 90 >= EXTRACT(EPOCH FROM NOW())";
            client.query(countPendingTransactions, [userID], function (err, result) {

                if (err) {
                    console.log("select");
                    response.status(500).json({error: "Internal Server Error"});
                    rollback(client, done);
                    return;
                }
                // TODO: Add back pending limit
                // if (result.rows[0].pending_transactions > 3) {
                //     response.status(429).json({error: "Too Many Requests"});
                //     rollback(client, done);
                //     return;
                // }

                createPendingTransaction(client, done)
            });
        }

        function createPendingTransaction(client, done) {
            var insertPendingTransaction = 'INSERT INTO public.pending_transaction VALUES ($1, $2, $3, $4, $5, $6)';
            client.query(insertPendingTransaction, [transactionID, userID, clientID, itemID, quantity, bread], function (err, result) {

                if (err) {
                    console.log("insert: " + err);
                    response.status(500).json({error: "Internal Server Error"});
                    rollback(client, done);
                    return;
                }

                commit(client, done);
                response.status(200).json({transaction_id: transactionID});
            });
        }

        pool.connect(function(err, client, done) {
            begin(client, done, setupTransaction);
        });
    }

    firebase.auth().verifyIdToken(token).then(function(decodedToken) {
        createTransaction(decodedToken.uid);
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

                if (!row) {
                    response.status(400).json({error: "Bad Request"});
                    console.log("Transaction ID does not exist: " + transactionID);
                    return;
                }

                var now = (new Date).getTime() / 1000;
                var created_datetime = row.created_datetime;

                if (now - created_datetime > 90) {
                    response.status(410).json({error: "Gone: Transaction Expired"});
                    return;
                }

                var clientID = row.client_id;
                var itemID = row.item_id;
                var quantity = row.quantity;
                var bread = row.bread;
                var userID = row.user_id;

                if (userID != tokenUserID) {
                    response.status(401).json({error: "Unauthorized"});
                    return;
                }

                var beginningBalance = 0;
                var userPantry = firebaseDB.ref('users/' + userID + '/pantry/bread_balance');
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

                        var userItem = firebaseDB.ref('users/' + userID + '/clients/' + clientID + '/' + itemID);
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
                                movePendingTransaction(transactionID, "fulfilled_transaction", response, function() {
                                    saveToFirebase(transactionID, clientID, itemID, quantity, bread, userID, parseInt(created_datetime), function() {
                                        console.log("replying with success message");
                                        response.status(200).json({result: "Transaction Successfully Completed"});
                                    });
                                });
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

app.post('/cancel_transaction', jsonParser, function(request, response) {
    if (!request.body) {
        response.status(400).json({error: "Bad Request"});
        return;
    }

    var transactionID = request.body.transaction_id;
    var token         = request.body.user_token;

    firebase.auth().verifyIdToken(token).then(function(decodedToken) {
        movePendingTransaction(transactionID, "cancelled_transaction", response, function() {
            response.status(200).json({result: "Transaction Successfully Cancelled"});
        });
    }).catch(function(error) {
        console.log(error);
        response.status(401).json({error: "Unauthorized"});
    })
});

app.post('/verify_otp', jsonParser, function(request, response) {
    if (!request.body) {
        response.status(400).json({error: "Bad Request"});
        return;
    }

    var token    = request.body.user_token;
    var clientID = request.body.client_id;
    var otp      = request.body.otp;

    function dec2hex(s) { return (s < 15.5 ? '0' : '') + Math.round(s).toString(16); }
    function hex2dec(s) { return parseInt(s, 16); }

    function base32tohex(base32) {
        var base32chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        var bits = "";
        var hex = "";

        for (var i = 0; i < base32.length; i++) {
            var val = base32chars.indexOf(base32.charAt(i).toUpperCase());
            bits += leftpad(val.toString(2), 5, '0');
        }

        for (var i = 0; i+4 <= bits.length; i+=4) {
            var chunk = bits.substr(i, 4);
            hex = hex + parseInt(chunk, 2).toString(16) ;
        }
        return hex;

    }

    function leftpad(str, len, pad) {
        if (len + 1 >= str.length) {
            str = Array(len + 1 - str.length).join(pad) + str;
        }
        return str;
    }

    function checkOTP(key, epoch, candidateOTP) {
        var time = leftpad(dec2hex(Math.floor(epoch / 30)), 16, '0');

        var shaObj = new jsSHA("SHA-1", "HEX");
        shaObj.setHMACKey(key, "HEX");
        shaObj.update(time);
        var hmac = shaObj.getHMAC("HEX");
        
        var offset = hex2dec(hmac.substring(hmac.length - 1));
        var part1 = hmac.substr(0, offset * 2);
        var part2 = hmac.substr(offset * 2, 8);
        var part3 = hmac.substr(offset * 2 + 8, hmac.length - offset);

        var otp = (hex2dec(hmac.substr(offset * 2, 8)) & hex2dec('7fffffff')) + '';
        var otp1 = (otp).substr(otp.length - 5, 5);
        var otp2 = (otp).substr(0, 4);

        console.log("CandidateOTP: " + candidateOTP);
        console.log("OTP1: " + otp1);
        console.log("OTP2: " + otp2);

        if (candidateOTP == otp1) {
            return otp2;
        } else {
            return null;
        }
    }

    function verifyOTP() {
        var clientRef = firebaseDB.ref("clients/" + clientID + "/transaction_secret_key");
        clientRef.once("value").then(function(snapshot) {
            function notNull(val) {
                return (val != null);
            }

            var secret = snapshot.val();
            var key = base32tohex(secret);
    
            var epoch1 = Math.round(new Date().getTime() / 1000.0);
            var epoch2 = epoch1 - 30;
            var epoch3 = epoch1 + 30;
    
            var result1 = checkOTP(key, epoch1, otp);
            var result2 = checkOTP(key, epoch2, otp);
            var result3 = checkOTP(key, epoch3, otp);
    
            var results = [result1, result2, result3];
    
            var validResults = results.filter(notNull);

            if (validResults.length != 1) {
                console.log("invalid");
                response.status(200).json({
                    result: {
                        verified: false
                    }
                });
            } else {
                console.log("valid");
                response.status(200).json({
                    result: {
                        verified: true,
                        confirmation_code: validResults[0]
                    }
                });
            }
        });
    }

    firebase.auth().verifyIdToken(token).then(function(decodedToken) {
        verifyOTP();
    }).catch(function(error) {
        console.log(error);
        response.status(401).json({error: "Unauthorized"});
    })

});