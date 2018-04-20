'use strict';

const Eventstore = require('../../../src/inmemory/Eventstore'),
      getTestsFor = require('../getTestsFor');

suite.skip('inmemory/Eventstore', function () {
  this.timeout(90 * 1000);

  getTestsFor(Eventstore, {
    type: 'inmemory'
  });
});
