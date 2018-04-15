'use strict';

const shell = require('shelljs');

const post = async function () {
  shell.exec([
    'docker kill mariadb-integration',
    'docker kill mongodb-integration',
    'docker kill mysql-integration',
    'docker kill postgres-integration',
    'docker rm -v mariadb-integration',
    'docker rm -v mongodb-integration',
    'docker rm -v mysql-integration',
    'docker rm -v postgres-integration'
  ].join(';'));
};

module.exports = post;
