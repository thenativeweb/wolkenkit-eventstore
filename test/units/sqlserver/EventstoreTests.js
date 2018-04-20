'use strict';

const shell = require('shelljs');

const env = require('../../shared/env'),
      Eventstore = require('../../../src/sqlserver/Eventstore'),
      getTestsFor = require('../getTestsFor'),
      waitForSqlServer = require('../../shared/waitForSqlServer');

suite('sqlserver/Eventstore', () => {
  getTestsFor(Eventstore, {
    url: env.SQLSERVER_URL_UNITS,
    nonExistentUrl: 'mssql://user:password@localhost:1433/non-existent',

    async startContainer () {
      shell.exec('docker start sqlserver-units');
      await waitForSqlServer({ url: env.SQLSERVER_URL_UNITS });
    },

    async stopContainer () {
      shell.exec('docker kill sqlserver-units');
    }
  });
});
