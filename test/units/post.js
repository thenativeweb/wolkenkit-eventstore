'use strict';

const processenv = require('processenv'),
      shell = require('shelljs');

const post = function (done) {
  if (processenv('CIRCLECI')) {
    // On CircleCI, we are not allowed to remove Docker containers.
    return done(null);
  }

  shell.exec('docker kill mongodb-units; docker rm -v mongodb-units; docker kill postgres-units; docker rm -v postgres-units', done);
};

module.exports = post;
