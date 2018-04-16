'use strict';

const env = require('../../shared/env'),
      getTestsFor = require('../getTestsFor'),
      Eventstore = require('../../../src/mysql/Eventstore');

suite('mysql/Eventstore', function () {
  this.timeout(90 * 1000);

  getTestsFor(Eventstore, {
    url: env.MYSQL_URL_PERFORMANCE,
    type: 'mysql'
  });
});
