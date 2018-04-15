'use strict';

const getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../src/inmemory/Sparbuch');

suite('inmemory/Sparbuch', () => {
  getTestsFor(Sparbuch, {
    type: 'inmemory'
  });
});
