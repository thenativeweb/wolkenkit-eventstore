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
      -p 3307:3306
      -e MYSQL_ROOT_PASSWORD=wolkenkit
      -e MYSQL_USER=wolkenkit
      -e MYSQL_PASSWORD=wolkenkit
      -e MYSQL_DATABASE=wolkenkit
      --name mariadb-units
      mariadb:10.3.5
      --bind-address=0.0.0.0
  `);
  shell.exec(oneLine`
    docker run
      -d
      -p 27018:27017
      -e MONGODB_DATABASE=wolkenkit
      -e MONGODB_USER=wolkenkit
      -e MONGODB_PASS=wolkenkit
      --name mongodb-units
      thenativeweb/wolkenkit-mongodb:latest
  `);
  shell.exec(oneLine`
    docker run
      -d
      -p 3310:3306
      -e MYSQL_ROOT_PASSWORD=wolkenkit
      -e MYSQL_USER=wolkenkit
      -e MYSQL_PASSWORD=wolkenkit
      -e MYSQL_DATABASE=wolkenkit
      --name mysql-units
      mysql:5.7.21
      --bind-address=0.0.0.0
  `);
  shell.exec(oneLine`
    docker run
      -d
      -p 5433:5432
      -e POSTGRES_DB=wolkenkit
      -e POSTGRES_USER=wolkenkit
      -e POSTGRES_PASSWORD=wolkenkit
      --name postgres-units
      thenativeweb/wolkenkit-postgres:latest
  `);
  shell.exec(oneLine`
    docker run
      -d
      -p 1433:1433
      -e ACCEPT_EULA=Y
      -e SA_PASSWORD=Wolkenkit123
      --name sqlserver-units
      microsoft/mssql-server-linux:2017-CU6
  `);

  await waitForMaria({ url: env.MARIA_URL_UNITS });
  await waitForMongo({ url: env.MONGO_URL_UNITS });
  await waitForMysql({ url: env.MYSQL_URL_UNITS });
  await waitForPostgres({ url: env.POSTGRES_URL_UNITS });
  await waitForSqlServer({ url: env.SQLSERVER_URL_UNITS });
};

module.exports = pre;
