'use strict';

const oneLine = require('common-tags/lib/oneLine'),
      shell = require('shelljs');

const env = require('../shared/env'),
      waitForMaria = require('../shared/waitForMaria'),
      waitForMongo = require('../shared/waitForMongo'),
      waitForMysql = require('../shared/waitForMysql'),
      waitForPostgres = require('../shared/waitForPostgres'),
      waitForSqlServer = require('../shared/waitForSqlServer');

const pre = async function () {
  shell.exec(oneLine`
    docker run
      -d
      -p 3309:3306
      -e MYSQL_ROOT_PASSWORD=wolkenkit
      -e MYSQL_USER=wolkenkit
      -e MYSQL_PASSWORD=wolkenkit
      -e MYSQL_DATABASE=wolkenkit
      --name mariadb-performance
      mariadb:10.3.5
      --bind-address=0.0.0.0
  `);
  shell.exec(oneLine`
    docker run
      -d
      -p 27020:27017
      -e MONGODB_DATABASE=wolkenkit
      -e MONGODB_USER=wolkenkit
      -e MONGODB_PASS=wolkenkit
      --name mongodb-performance
      thenativeweb/wolkenkit-mongodb:latest
  `);
  shell.exec(oneLine`
    docker run
      -d
      -p 3312:3306
      -e MYSQL_ROOT_PASSWORD=wolkenkit
      -e MYSQL_USER=wolkenkit
      -e MYSQL_PASSWORD=wolkenkit
      -e MYSQL_DATABASE=wolkenkit
      --name mysql-performance
      mysql:5.7.21
      --bind-address=0.0.0.0
  `);
  shell.exec(oneLine`
    docker run
      -d
      -p 5435:5432
      -e POSTGRES_DB=wolkenkit
      -e POSTGRES_USER=wolkenkit
      -e POSTGRES_PASSWORD=wolkenkit
      --name postgres-performance
      thenativeweb/wolkenkit-postgres:latest
  `);
  shell.exec(oneLine`
    docker run
      -d
      -p 1435:1433
      -e ACCEPT_EULA=Y
      -e SA_PASSWORD=Wolkenkit123
      --name sqlserver-performance
      microsoft/mssql-server-linux:2017-CU6
  `);

  await waitForMaria({ url: env.MARIA_URL_PERFORMANCE });
  await waitForMongo({ url: env.MONGO_URL_PERFORMANCE });
  await waitForMysql({ url: env.MYSQL_URL_PERFORMANCE });
  await waitForPostgres({ url: env.POSTGRES_URL_PERFORMANCE });
  await waitForSqlServer({ url: env.SQLSERVER_URL_PERFORMANCE });
};

module.exports = pre;
