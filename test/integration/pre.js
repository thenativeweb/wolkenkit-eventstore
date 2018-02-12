'use strict';

const shell = require('shelljs');

const env = require('../helpers/env'),
      waitForMongo = require('../helpers/waitForMongo'),
      waitForPostgres = require('../helpers/waitForPostgres');

const pre = function (done) {
  (async () => {
    try {
      shell.exec('docker run -d -p 27019:27017 --name mongodb-integration mongo:3.4.2');
      shell.exec('docker run -d -p 5434:5432 -e POSTGRES_USER=wolkenkit -e POSTGRES_PASSWORD=wolkenkit -e POSTGRES_DB=wolkenkit --name postgres-integration postgres:9.6.4-alpine');

      await waitForMongo({ url: env.MONGO_URL_INTEGRATION });
      await waitForPostgres({ url: env.POSTGRES_URL_INTEGRATION });
    } catch (ex) {
      return done(ex);
    }
    done();
  })();
};

module.exports = pre;
