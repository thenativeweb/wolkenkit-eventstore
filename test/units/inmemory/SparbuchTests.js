'use strict';

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/inmemory/Sparbuch');

suite('inmemory/Sparbuch', () => {
  getTestsFor(Sparbuch, {
    url: env.INMEMORY_URL_UNITS,
    nonExistentUrl: '/dev/null',

    /* eslint-disable */
    async startContainer () {
      return true;
    },

    async stopContainer () {
      return true;
    }
    /* eslint-enable */
  });
});
