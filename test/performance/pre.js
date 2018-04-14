'use strict';

const shell = require('shelljs');

const env = require('../shared/env'),
      waitForMaria = require('../shared/waitForMaria'),
      waitForMongo = require('../shared/waitForMongo'),
      waitForMysql = require('../shared/waitForMysql'),
      waitForPostgres = require('../shared/waitForPostgres');

const pre = async function () {
  shell.exec('docker run -d -p 3309:3306 -e MYSQL_ROOT_PASSWORD=wolkenkit -e MYSQL_USER=wolkenkit -e MYSQL_PASSWORD=wolkenkit -e MYSQL_DATABASE=wolkenkit --name mariadb-performance mariadb:10.3.5 --bind-address=0.0.0.0');
  shell.exec('docker run -d -p 27020:27017 --name mongodb-performance mongo:3.4.2');
  shell.exec('docker run -d -p 3312:3306 -e MYSQL_ROOT_PASSWORD=wolkenkit -e MYSQL_USER=wolkenkit -e MYSQL_PASSWORD=wolkenkit -e MYSQL_DATABASE=wolkenkit --name mysql-performance mysql:5.7.21 --bind-address=0.0.0.0');
  shell.exec('docker run -d -p 5435:5432 -e POSTGRES_USER=wolkenkit -e POSTGRES_PASSWORD=wolkenkit -e POSTGRES_DB=wolkenkit --name postgres-performance postgres:9.6.4-alpine');

  await waitForMaria({ url: env.MARIA_URL_PERFORMANCE });
  await waitForMongo({ url: env.MONGO_URL_PERFORMANCE });
  await waitForMysql({ url: env.MYSQL_URL_PERFORMANCE });
  await waitForPostgres({ url: env.POSTGRES_URL_PERFORMANCE });
};

module.exports = pre;
