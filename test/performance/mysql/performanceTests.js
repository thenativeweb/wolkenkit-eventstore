'use strict';

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/mysql/Sparbuch');

suite.only('mysql/Sparbuch', function () {
  this.timeout(15 * 1000);

  getTestsFor(Sparbuch, {
    url: env.MYSQL_URL_PERFORMANCE,
    type: 'mysql'
  });
});
