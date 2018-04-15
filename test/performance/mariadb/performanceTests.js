'use strict';

const env = require('../../shared/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../src/mariadb/Sparbuch');

suite('mariadb/Sparbuch', function () {
  this.timeout(90 * 1000);

  getTestsFor(Sparbuch, {
    url: env.MARIA_URL_PERFORMANCE,
    type: 'mariadb'
  });
});
