'use strict';

const shell = require('shelljs');

const env = require('../helpers/env'),
      waitForMongo = require('../helpers/waitForMongo'),
      waitForPostgres = require('../helpers/waitForPostgres');

const pre = function (done) {
  shell.exec('docker run -d -p 27018:27017 --name mongodb-units mongo:3.4.2', errMongo => {
    if (errMongo) {
      return done(errMongo);
    }

    shell.exec('docker run -d -p 5433:5432 -e POSTGRES_USER=wolkenkit -e POSTGRES_PASSWORD=wolkenkit -e POSTGRES_DB=wolkenkit --name postgres-units postgres:9.6.2-alpine', errPostgres => {
      if (errPostgres) {
        return done(errPostgres);
      }

      waitForMongo({ url: env.MONGO_URL_UNITS }, errWaitForMongoDb => {
        if (errWaitForMongoDb) {
          return done(errWaitForMongoDb);
        }

        waitForPostgres({ url: env.POSTGRES_URL_UNITS }, errWaitForPostgres => {
          if (errWaitForPostgres) {
            return done(errWaitForPostgres);
          }

          done(null);
        });
      });
    });
  });
};

module.exports = pre;
