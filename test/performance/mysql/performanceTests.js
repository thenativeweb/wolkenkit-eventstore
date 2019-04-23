'use strict';

const env = require('../../shared/env'),
      Eventstore = require('../../../lib/mysql/Eventstore'),
      getTestsFor = require('../getTestsFor');

suite('mysql/Eventstore', function () {
  this.timeout(180 * 1000);

  getTestsFor(Eventstore, {
    url: env.MYSQL_URL_PERFORMANCE,
    type: 'mysql'
  });
});
