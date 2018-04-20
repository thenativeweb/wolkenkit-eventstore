'use strict';

const shell = require('shelljs');

const post = async function () {
  shell.exec([
    'docker kill mariadb-performance',
    'docker kill mongodb-performance',
    'docker kill mysql-performance',
    'docker kill postgres-performance',
    'docker kill sqlserver-performance',
    'docker rm -v mariadb-performance',
    'docker rm -v mongodb-performance',
    'docker rm -v mysql-performance',
    'docker rm -v postgres-performance',
    'docker rm -v sqlserver-performance'
  ].join(';'));
};

module.exports = post;
