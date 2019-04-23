'use strict';

const env = require('../../shared/env'),
      Eventstore = require('../../../lib/mongodb/Eventstore'),
      getTestsFor = require('../getTestsFor');

suite('mongodb/Eventstore', function () {
  this.timeout(90 * 1000);

  getTestsFor(Eventstore, {
    url: env.MONGO_URL_PERFORMANCE,
    type: 'mongodb'
  });
});
