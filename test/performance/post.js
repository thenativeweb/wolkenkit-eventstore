'use strict';

const shell = require('shelljs');

shell.exec([
  'docker kill mariadb-performance; docker rm -v mariadb-performance',
  'docker kill mongodb-performance; docker rm -v mongodb-performance',
  'docker kill mysql-performance; docker rm -v mysql-performance',
  'docker kill postgres-performance; docker rm -v postgres-performance'
].join(';'));
