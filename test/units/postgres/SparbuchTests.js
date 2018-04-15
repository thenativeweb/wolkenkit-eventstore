'use strict';

const shell = require('shelljs');

const env = require('../../shared/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/postgres/Sparbuch'),
      waitForPostgres = require('../../shared/waitForPostgres');

suite('postgres/Sparbuch', () => {
  getTestsFor(Sparbuch, {
    url: env.POSTGRES_URL_UNITS,
    nonExistentUrl: 'pg://localhost/non-existent',

    async startContainer () {
      shell.exec('docker start postgres-units');
      await waitForPostgres({ url: env.POSTGRES_URL_UNITS });
    },

    async stopContainer () {
      shell.exec('docker kill postgres-units');
    }
  });
});
