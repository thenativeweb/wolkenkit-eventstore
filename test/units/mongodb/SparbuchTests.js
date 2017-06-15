'use strict';

const shell = require('shelljs');

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/mongodb/Sparbuch'),
      waitForMongo = require('../../helpers/waitForMongo');

suite('mongodb/Sparbuch', () => {
  getTestsFor(Sparbuch, {
    url: env.MONGO_URL_UNITS,
    nonExistentUrl: 'mongodb://non-existent.thenativeweb/non-existent',

    startContainer (done) {
      shell.exec('docker start mongodb-units', statusCode => {
        if (statusCode) {
          return done(new Error(`Unexpected status code ${statusCode}.`));
        }
        waitForMongo({ url: env.MONGO_URL_UNITS }, done);
      });
    },

    stopContainer () {
      shell.exec('docker kill mongodb-units');
    }
  });
});
