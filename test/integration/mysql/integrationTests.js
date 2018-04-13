'use strict';

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor');

suite('mysql/integration', function () {
  this.timeout(10 * 1000);

  getTestsFor({
    type: 'mysql',
    url: env.MYSQL_URL_INTEGRATION
  });
});
