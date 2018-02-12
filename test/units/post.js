'use strict';

const shell = require('shelljs');

const post = function (done) {
  (async () => {
    try {
      shell.exec('docker kill mongodb-units; docker rm -v mongodb-units; docker kill postgres-units; docker rm -v postgres-units');
    } catch (ex) {
      return done(ex);
    }
    done();
  })();
};

module.exports = post;
