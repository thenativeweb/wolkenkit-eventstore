'use strict';

const env = require('../../helpers/env'),
      getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/inmemory/Sparbuch');

suite('inmemory/Sparbuch', function () {
  this.timeout(15 * 1000);

  getTestsFor(Sparbuch, {
    url: env.INMEMORY_URL_PERFORMANCE,
    type: 'inmemory'
  });
});
