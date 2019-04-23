'use strict';

const Eventstore = require('../../../lib/inmemory/Eventstore'),
      getTestsFor = require('../getTestsFor');

suite('inmemory/Eventstore', () => {
  getTestsFor(Eventstore, {
    type: 'inmemory'
  });
});
