'use strict';

const getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/inmemory/Sparbuch');

suite('inmemory/Sparbuch', function () {
  this.timeout(15 * 1000);

  getTestsFor(Sparbuch, {
    type: 'inmemory'
  });
});
