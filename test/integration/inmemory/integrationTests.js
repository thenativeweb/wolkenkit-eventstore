'use strict';

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor');

suite('inmemory/integration', function () {
  this.timeout(10 * 1000);

  getTestsFor({
    type: 'inmemory',
    url: env.INMEMORY_URL_INTEGRATION
  });
});
