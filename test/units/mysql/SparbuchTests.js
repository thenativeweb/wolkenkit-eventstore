'use strict';

const shell = require('shelljs');

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/mysql/Sparbuch'),
      waitForMaria = require('../../helpers/waitForMysql');

suite('mysql/Sparbuch', () => {
  getTestsFor(Sparbuch, {
    url: env.MYSQL_URL_UNITS,
    nonExistentUrl: 'mysql://localhost/non-existent',

    async startContainer () {
      shell.exec('docker start mysql-units');
      await waitForMaria({ url: env.MYSQL_URL_UNITS });
    },

    async stopContainer () {
      shell.exec('docker kill mysql-units');
    }
  });
});
