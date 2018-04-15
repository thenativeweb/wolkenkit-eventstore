'use strict';

const env = require('../../shared/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/postgres/Sparbuch');

suite('postgres/Sparbuch', function () {
  this.timeout(90 * 1000);

  getTestsFor(Sparbuch, {
    url: env.POSTGRES_URL_PERFORMANCE,
    type: 'postgres'
  });
});
