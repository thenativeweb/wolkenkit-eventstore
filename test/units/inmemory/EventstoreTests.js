'use strict';

const Eventstore = require('../../../src/inmemory/Eventstore'),
      getTestsFor = require('../getTestsFor');

suite('inmemory/Eventstore', () => {
  getTestsFor(Eventstore, {
    type: 'inmemory'
  });
});
