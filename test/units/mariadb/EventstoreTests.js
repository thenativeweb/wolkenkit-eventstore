'use strict';

const shell = require('shelljs');

const env = require('../../shared/env'),
      Eventstore = require('../../../src/mariadb/Eventstore'),
      getTestsFor = require('../getTestsFor'),
      waitForMaria = require('../../shared/waitForMaria');

suite('mariadb/Eventstore', () => {
  getTestsFor(Eventstore, {
    url: env.MARIA_URL_UNITS,

    async startContainer () {
      shell.exec('docker start mariadb-units');
      await waitForMaria({ url: env.MARIA_URL_UNITS });
    },

    async stopContainer () {
      shell.exec('docker kill mariadb-units');
    }
  });
});
