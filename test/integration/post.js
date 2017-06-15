'use strict';

const processenv = require('processenv'),
      shell = require('shelljs');

const post = function (done) {
  if (processenv('CIRCLECI')) {
    // On CircleCI, we are not allowed to remove Docker containers.
    return done(null);
  }

  shell.exec('docker kill mongodb-integration; docker rm -v mongodb-integration; docker kill postgres-integration; docker rm -v postgres-integration', done);
};

module.exports = post;
