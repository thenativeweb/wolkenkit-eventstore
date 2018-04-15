'use strict';

const getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../src/inmemory/Sparbuch');

suite('inmemory/Sparbuch', function () {
  this.timeout(90 * 1000);

  getTestsFor(Sparbuch, {
    type: 'inmemory'
  });
});
