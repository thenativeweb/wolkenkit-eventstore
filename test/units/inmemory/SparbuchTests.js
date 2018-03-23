'use strict';

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/inmemory/Sparbuch');

suite('inmemory/Sparbuch', () => {
  getTestsFor(Sparbuch, {
    url: env.INMEMORY_URL_UNITS,
    type: 'inmemory',
    nonExistentUrl: '/dev/null',

    async startContainer () {
      return true;
    },

    async stopContainer () {
      return true;
    }
  });
});
