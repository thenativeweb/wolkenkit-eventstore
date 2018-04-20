'use strict';

const env = require('../../shared/env'),
      Eventstore = require('../../../src/sqlserver/Eventstore'),
      getTestsFor = require('../getTestsFor');

suite('sqlserver/Eventstore', function () {
  this.timeout(90 * 1000);

  getTestsFor(Eventstore, {
    url: env.SQLSERVER_URL_PERFORMANCE,
    type: 'sqlserver'
  });
});
