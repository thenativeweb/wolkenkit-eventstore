'use strict';

const shell = require('shelljs');

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/postgres/Sparbuch'),
      waitForPostgres = require('../../helpers/waitForPostgres');

suite('postgres/Sparbuch', () => {
  getTestsFor(Sparbuch, {
    url: env.POSTGRES_URL_UNITS,
    nonExistentUrl: 'pg://localhost/non-existent',

    startContainer (done) {
      shell.exec('docker start postgres-units', statusCode => {
        if (statusCode) {
          return done(new Error(`Unexpected status code ${statusCode}.`));
        }
        waitForPostgres({ url: env.POSTGRES_URL_UNITS }, done);
      });
    },

    stopContainer () {
      shell.exec('docker kill postgres-units');
    }
  });
});
