'use strict';

const env = require('../../shared/env'),
      getTestsFor = require('../getTestsFor'),
      Eventstore = require('../../../src/mongodb/Eventstore');

suite('mongodb/Eventstore', function () {
  this.timeout(90 * 1000);

  getTestsFor(Eventstore, {
    url: env.MONGO_URL_PERFORMANCE,
    type: 'mongodb'
  });
});
