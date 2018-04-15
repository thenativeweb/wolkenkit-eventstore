'use strict';

const env = require('../../shared/env'),
      getTestsFor = require('../getTestsFor');

suite('mariadb/integration', function () {
  this.timeout(10 * 1000);

  getTestsFor({
    type: 'mariadb',
    url: env.MARIA_URL_INTEGRATION
  });
});
