'use strict';

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor');

suite('postgres/integration', function () {
  this.timeout(15 * 1000);

  getTestsFor({
    type: 'postgres',
    url: env.POSTGRES_URL_INTEGRATION
  });
});
