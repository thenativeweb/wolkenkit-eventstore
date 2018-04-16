'use strict';

const env = require('../../shared/env'),
      getTestsFor = require('../getTestsFor'),
      Eventstore = require('../../../src/postgres/Eventstore');

suite('postgres/Eventstore', function () {
  this.timeout(90 * 1000);

  getTestsFor(Eventstore, {
    url: env.POSTGRES_URL_PERFORMANCE,
    type: 'postgres'
  });
});
