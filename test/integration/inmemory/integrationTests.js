'use strict';

const getTestsFor = require('../getTestsFor');

suite('inmemory/integration', function () {
  this.timeout(10 * 1000);

  getTestsFor({
    type: 'inmemory'
  });
});
