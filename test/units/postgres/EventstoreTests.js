'use strict';

const shell = require('shelljs');

const env = require('../../shared/env'),
      Eventstore = require('../../../src/postgres/Eventstore'),
      getTestsFor = require('../getTestsFor'),
      waitForPostgres = require('../../shared/waitForPostgres');

suite('postgres/Eventstore', () => {
  getTestsFor(Eventstore, {
    url: env.POSTGRES_URL_UNITS,

    async startContainer () {
      shell.exec('docker start postgres-units');
      await waitForPostgres({ url: env.POSTGRES_URL_UNITS });
    },

    async stopContainer () {
      shell.exec('docker kill postgres-units');
    }
  });
});
