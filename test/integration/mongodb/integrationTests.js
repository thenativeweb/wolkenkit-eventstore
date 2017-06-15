'use strict';

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor');

suite('mongodb/integration', function () {
  this.timeout(90 * 1000);

  getTestsFor({
    type: 'mongodb',
    url: env.MONGO_URL_INTEGRATION
  });
});
