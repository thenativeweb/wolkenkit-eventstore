'use strict';

const shell = require('shelljs');

const post = function (done) {
  (async () => {
    try {
      shell.exec('docker kill mongodb-integration; docker rm -v mongodb-integration; docker kill postgres-integration; docker rm -v postgres-integration');
    } catch (ex) {
      return done(ex);
    }
    done();
  })();
};

module.exports = post;
