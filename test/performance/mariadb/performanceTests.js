'use strict';

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/mariadb/Sparbuch');

suite('mariadb/Sparbuch', function () {
  this.timeout(15 * 1000);

  getTestsFor(Sparbuch, {
    url: env.MARIA_URL_PERFORMANCE,
    type: 'mariadb'
  });
});
