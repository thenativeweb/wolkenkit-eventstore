'use strict';

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/mongodb/Sparbuch');

suite('mongodb/Sparbuch', function () {
  this.timeout(90 * 1000);

  getTestsFor(Sparbuch, {
    url: env.MONGO_URL_PERFORMANCE,
    type: 'mongodb'
  });
});
