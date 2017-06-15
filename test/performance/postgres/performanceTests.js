'use strict';

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/postgres/Sparbuch');

suite('postgres/Sparbuch', function () {
  this.timeout(15 * 1000);

  getTestsFor(Sparbuch, {
    url: env.POSTGRES_URL_PERFORMANCE,
    type: 'postgres'
  });
});
