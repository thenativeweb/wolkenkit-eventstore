'use strict';

const processenv = require('processenv'),
      shell = require('shelljs');

if (processenv('CIRCLECI')) {
  // On CircleCI, we are not allowed to remove Docker containers.

  /* eslint-disable no-process-exit */
  process.exit(0);
  /* eslint-enable no-process-exit */
}

shell.exec([
  'docker kill mongodb-performance; docker rm -v mongodb-performance',
  'docker kill postgres-performance; docker rm -v postgres-performance'
].join(';'));
