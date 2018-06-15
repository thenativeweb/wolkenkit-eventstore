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
      -p 3308:3306
      -e MYSQL_ROOT_PASSWORD=wolkenkit
      -e MYSQL_USER=wolkenkit
      -e MYSQL_PASSWORD=wolkenkit
      -e MYSQL_DATABASE=wolkenkit
      --name mariadb-integration
      mariadb:10.3.5
      --bind-address=0.0.0.0
  `);
  shell.exec(oneLine`
    docker run
      -d
      -p 27019:27017
      -e MONGODB_DATABASE=wolkenkit
      -e MONGODB_USER=wolkenkit
      -e MONGODB_PASS=wolkenkit
      --name mongodb-integration
      thenativeweb/wolkenkit-mongodb:latest
  `);
  shell.exec(oneLine`
    docker run
      -d
      -p 3311:3306
      -e MYSQL_ROOT_PASSWORD=wolkenkit
      -e MYSQL_USER=wolkenkit
      -e MYSQL_PASSWORD=wolkenkit
      -e MYSQL_DATABASE=wolkenkit
      --name mysql-integration
      mysql:5.7.21
      --bind-address=0.0.0.0
  `);
  shell.exec(oneLine`
    docker run
      -d
      -p 5434:5432
      -e POSTGRES_DB=wolkenkit
      -e POSTGRES_USER=wolkenkit
      -e POSTGRES_PASSWORD=wolkenkit
      --name postgres-integration
      thenativeweb/wolkenkit-postgres:latest
  `);
  shell.exec(oneLine`
    docker run
      -d
      -p 1434:1433
      -e ACCEPT_EULA=Y
      -e SA_PASSWORD=Wolkenkit123
      --name sqlserver-integration
      microsoft/mssql-server-linux:2017-CU6
  `);

  await waitForMaria({ url: env.MARIA_URL_INTEGRATION });
  await waitForMongo({ url: env.MONGO_URL_INTEGRATION });
  await waitForMysql({ url: env.MYSQL_URL_INTEGRATION });
  await waitForPostgres({ url: env.POSTGRES_URL_INTEGRATION });
  await waitForSqlServer({ url: env.SQLSERVER_URL_INTEGRATION });
};

module.exports = pre;
