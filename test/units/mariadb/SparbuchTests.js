'use strict';

const shell = require('shelljs');

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/mariadb/Sparbuch'),
      waitForMaria = require('../../helpers/waitForMaria');

suite('mariadb/Sparbuch', () => {
  getTestsFor(Sparbuch, {
    url: env.MARIA_URL_UNITS,
    nonExistentUrl: 'mariadb://localhost/non-existent',

    async startContainer () {
      shell.exec('docker start mariadb-units');
      await waitForMaria({ url: env.MARIA_URL_UNITS });
    },

    async stopContainer () {
      shell.exec('docker kill mariadb-units');
    }
  });
});
