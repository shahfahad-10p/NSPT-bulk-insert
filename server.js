const addresses = require('./m3-customer-address-data.json');
const customers = require('./m3-customer-data.json');
const async = require('async');
const mongodb = require('mongodb')
const url = 'mongodb://localhost:27017/NSPT-db'

const insertSize = parseInt(process.argv[2], 10) || 100
let pack = customers.length / (customers.length / insertSize);
let packArray = [];
let tasks = [];

mongodb.MongoClient.connect(url, (error, db) => {
    if (error) {
        console.log("ERROR CONNECTING DB");
        return process.exit(1)
    }
    console.log("DB CONNECTION SUCCESSFUL");
    console.log('MAKING PACKS OF SIZE : ', pack);

    db.collection('customers').remove();

    for (let index = 0; index < customers.length; index++) {
        customers[index] = Object.assign(customers[index], addresses[index]);
        packArray.push(index);
        if (packArray.length === pack) {
            const start = packArray[0];
            const end = packArray[packArray.length - 1];
            packArray = [];
            tasks.push((done) => {
                console.log('INSERTING FROM ' + start + ' TO ' + end);
                db.collection('customers').insert(customers.slice(start, end), (error, results) => {
                    done(error, results);
                })
            });
        }
    }

    const startTime = Date.now();
    async.parallel(tasks, (error, result) => {
        if (error) {
            console.log('ASYNC ERROR : ', error);
            process.exit(1);
        }
        db.collection('customers').count().then(res => {
            const endTime = Date.now();
            console.log('TIME TAKEN : ', endTime - startTime);
            db.close();
        });
    });

});