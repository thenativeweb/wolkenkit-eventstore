'use strict';

const shell = require('shelljs');

const env = require('../../shared/env'),
      Eventstore = require('../../../src/mongodb/Eventstore'),
      getTestsFor = require('../getTestsFor'),
      waitForMongo = require('../../shared/waitForMongo');

suite('mongodb/Eventstore', () => {
  getTestsFor(Eventstore, {
    url: env.MONGO_URL_UNITS,

    async startContainer () {
      shell.exec('docker start mongodb-units');
      await waitForMongo({ url: env.MONGO_URL_UNITS });
    },

    async stopContainer () {
      shell.exec('docker kill mongodb-units');
    }
  });
});
