'use strict';
global.CONFIG_FILE_PATH = __dirname + '/config.json';

const LOOKUP_QUEUE_URL = 'https://sqs.us-west-2.amazonaws.com/810415707352/product_lookup_asin';

const SEARCH_HOSTNAME = 'webservices.amazon.com';
const SEARCH_PATH = '/onca/xml';

const AWS_ACCESS_KEY = 'XXXXXX';
const AWS_SECRET_KEY = 'XXXXXX+XXXXXX';

let AWS = require('aws-sdk');
let SQS = new AWS.SQS({apiVersion: '2012-11-05'});
let http = require('http');
let querystring = require('querystring');
let parseString = require('xml2js').parseString;
let amazonSignature = require('tg-node-lib/lib/amazonSignature');
let dbCatalog = require('tg-node-lib/lib/db/catalog');

// vars that need to be reset
let timeout = 0;
let recentlyAddedASINs = {};

exports.handler = (event, context, callback) => {
    timeout = Date.now() + 300000; // 5m
    recentlyAddedASINs = {};
    // setup returns a promise
    // doing setup each instance might be overkill, but it is better than doing it every time a table is loaded
    dbCatalog.setup()
        .then(() => {
            return poll()
        })
        .then((res) => callback(null, res))
        .catch((err) => callback(err, null));
};

function poll() {
    console.log('-- Poll Queue');
    return new Promise((resolve, reject) => {
        SQS.receiveMessage({
            QueueUrl: LOOKUP_QUEUE_URL,
            // MaxNumberOfMessages: 1,
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: 1
        }, (err, data) => {
            if (err)
                return reject(err);

            // resolve when no more messages left
            if (typeof data.Messages === 'undefined')
                return resolve('Done');

            // resolve when no more messages left
            if (data.Messages.length < 1)
                return resolve('Done');

            var p = new Promise((resolve) => {
                // do nothing, simply start the chain
                resolve(true);
            });

            for (let i = 0; i < data.Messages.length; i++) {
                p = p.then(() => {
                    return lookup(data.Messages[i].Body);
                });
                p = p.then((result) => {
                    if (result === true)
                        deleteMessage(data.Messages[i]);
                    // return nothing
                })
            }

            p.then(() => {
                // poll again for more messages
                if (Date.now() < timeout) {
                    poll().then((res) => resolve(res)).catch((err) => reject(err));
                } else {
                    resolve('Done');
                }
            }).catch((err) => reject(err));
        })
    });
}

function deleteMessage(Message) {
    console.log('--- Delete Message');
    SQS.deleteMessage({
        QueueUrl: LOOKUP_QUEUE_URL,
        ReceiptHandle: Message.ReceiptHandle
    }, (err) => {
        if (err) throw err;
    });
}

function addToLookupQueueIfNotExists(asin) {
    return dbCatalog.Product().count({where: {asin: asin}})
        .then((count) => {
            if (count > 0) throw new Error('ASIN already exists, do not loop add it.');
            if (recentlyAddedASINs[asin] == true) throw new Error('ASIN already queued');
        })
        .then(() => {
            console.log('---- Queue ASIN -> ' + asin);
            return new Promise((resolve) => {
                SQS.sendMessage({
                    MessageBody: asin,
                    QueueUrl: LOOKUP_QUEUE_URL
                }, (err) => {
                    if (err)
                        throw err;
                    resolve(true);
                });
            });
        })
        .catch(() => {
            return false;
        });
}

function lookup(asin) {
    console.log('--- Lookup ASIN -> ' + asin);
    return new Promise((resolve, reject) => {
        // A-Z a-z sort is required for the signature
        let params = {
            AWSAccessKeyId: AWS_ACCESS_KEY, // first because upper case is before lower case
            AssociateTag: 'tokengoods-20',
            Condition: 'New',
            IdType: 'ASIN',
            ItemId: asin,
            Operation: 'ItemLookup',
            ResponseGroup: 'Accessories,BrowseNodes,EditorialReview,Images,ItemAttributes,Request,SalesRank,Similarities,Small',
            Timestamp: amazonSignature.getSigningTimestamp()
        };

        params.Signature = amazonSignature.getSignature('GET', SEARCH_HOSTNAME, SEARCH_PATH, querystring.stringify(params), AWS_SECRET_KEY);

        var req = http.request({
            hostname: SEARCH_HOSTNAME,
            path: SEARCH_PATH + "?" + querystring.stringify(params)
        }, (req) => {
            let resBody = '';
            req.on('data', (data) => {
                resBody += data;
            });
            req.on('end', () => {
                parseString(resBody, {
                    explicitArray: false // no super arrays
                }, (err, result) => {
                    if (err)
                        throw err;
                    resolve(result);
                });
            })
        });

        req.on('error', (err) => reject(err));
        req.end();
    }).then((result) => {
        if (typeof result.ItemLookupResponse === 'undefined'
            ||
            typeof result.ItemLookupResponse.Items === 'undefined'
            ||
            typeof result.ItemLookupResponse.Items.Item === 'undefined'
            ||
            result.ItemLookupResponse.Items.Request.IsValid !== 'True'
        ) {
            return false; // something went wrong, keep it in the queue
        }
        // promise
        console.log('---- Save Item');
        return dbCatalog.importAmazonItem(result.ItemLookupResponse.Items.Item);
    }).then((product) => {
        if (product === false)
            return false;
        // add related asins to the queue
        var newAsins = [];
        if (typeof product.amzItem.SimilarProducts != 'undefined') {
            for (var i = 0; i < product.amzItem.SimilarProducts.SimilarProduct.length; i++) {
                newAsins.push(addToLookupQueueIfNotExists(product.amzItem.SimilarProducts.SimilarProduct[i].ASIN));
            }
        }
        if (typeof product.amzItem.Accessories != 'undefined') {
            for (var i = 0; i < product.amzItem.Accessories.Accessory.length; i++) {
                newAsins.push(addToLookupQueueIfNotExists(product.amzItem.Accessories.Accessory[i].ASIN));
            }
        }
        return Promise.all(newAsins);
    }).then(() => {
        return true;
    }).catch((err) => {
        console.log(err);
        return false;
    });
}
