'use strict';

const env = require('../../shared/env'),
      getTestsFor = require('../getTestsFor'),
      Eventstore = require('../../../src/mariadb/Eventstore');

suite('mariadb/Eventstore', function () {
  this.timeout(90 * 1000);

  getTestsFor(Eventstore, {
    url: env.MARIA_URL_PERFORMANCE,
    type: 'mariadb'
  });
});
