'use strict';

const getTestsFor = require('../getTestsFor'),
      Sparbuch = require('../../../lib/inmemory/Sparbuch');

suite('inmemory/Sparbuch', () => {
  getTestsFor(Sparbuch, {
    type: 'inmemory'
  });
});
