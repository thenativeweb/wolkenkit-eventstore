'use strict';

const shell = require('shelljs');

const post = function (done) {
  (async () => {
    try {
      shell.exec('docker kill mariadb-units; docker rm -v mariadb-units; docker kill mongodb-units; docker rm -v mongodb-units; docker kill mysql-units; docker rm -v mysql-units; docker kill postgres-units; docker rm -v postgres-units');
    } catch (ex) {
      return done(ex);
    }
    done();
  })();
};

module.exports = post;
