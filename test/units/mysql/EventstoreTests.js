'use strict';

const shell = require('shelljs');

const env = require('../../shared/env'),
      Eventstore = require('../../../src/mysql/Eventstore'),
      getTestsFor = require('../getTestsFor'),
      waitForMaria = require('../../shared/waitForMysql');

suite('mysql/Eventstore', () => {
  getTestsFor(Eventstore, {
    url: env.MYSQL_URL_UNITS,

    async startContainer () {
      shell.exec('docker start mysql-units');
      await waitForMaria({ url: env.MYSQL_URL_UNITS });
    },

    async stopContainer () {
      shell.exec('docker kill mysql-units');
    }
  });
});
