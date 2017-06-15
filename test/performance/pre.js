'use strict';

const async = require('async'),
      shell = require('shelljs');

const env = require('../helpers/env'),
      waitForMongo = require('../helpers/waitForMongo'),
      waitForPostgres = require('../helpers/waitForPostgres');

async.series({
  runMongodb (callback) {
    shell.exec('docker run -d -p 27020:27017 --name mongodb-performance mongo:3.4.2', callback);
  },
  waitForMongodb (callback) {
    waitForMongo({ url: env.MONGO_URL_PERFORMANCE }, callback);
  },
  runPostgres (callback) {
    shell.exec('docker run -d -p 5435:5432 -e POSTGRES_USER=wolkenkit -e POSTGRES_PASSWORD=wolkenkit -e POSTGRES_DB=wolkenkit --name postgres-performance postgres:9.6.2-alpine', callback);
  },
  waitForPostgres (callback) {
    waitForPostgres({ url: env.POSTGRES_URL_PERFORMANCE }, callback);
  }
}, err => {
  if (err) {
    /* eslint-disable no-process-exit */
    process.exit(1);
    /* eslint-enable no-process-exit */
  }
});
