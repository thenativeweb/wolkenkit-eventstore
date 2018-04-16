'use strict';

const getTestsFor = require('../getTestsFor'),
      Eventstore = require('../../../src/inmemory/Eventstore');

suite('inmemory/Eventstore', () => {
  getTestsFor(Eventstore, {
    type: 'inmemory'
  });
});
