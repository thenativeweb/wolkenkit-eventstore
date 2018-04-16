'use strict';

const env = require('../../shared/env'),
      Eventstore = require('../../../src/mariadb/Eventstore'),
      getTestsFor = require('../getTestsFor');

suite('mariadb/Eventstore', function () {
  this.timeout(90 * 1000);

  getTestsFor(Eventstore, {
    url: env.MARIA_URL_PERFORMANCE,
    type: 'mariadb'
  });
});
