'use strict';

const getTestsFor = require('../getTestsFor'),
      Eventstore = require('../../../src/inmemory/Eventstore');

suite('inmemory/Eventstore', function () {
  this.timeout(90 * 1000);

  getTestsFor(Eventstore, {
    type: 'inmemory'
  });
});
