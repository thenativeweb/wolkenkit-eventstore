'use strict';

const shell = require('shelljs');

shell.exec([
  'docker kill mongodb-performance; docker rm -v mongodb-performance',
  'docker kill postgres-performance; docker rm -v postgres-performance'
].join(';'));
