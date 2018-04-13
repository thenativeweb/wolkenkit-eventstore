'use strict';

const shell = require('shelljs');

const env = require('../helpers/env'),
      waitForMaria = require('../helpers/waitForMaria'),
      waitForMongo = require('../helpers/waitForMongo'),
      waitForMysql = require('../helpers/waitForMysql'),
      waitForPostgres = require('../helpers/waitForPostgres');

const pre = function (done) {
  (async () => {
    try {
      shell.exec('docker run -d -p 3307:3306 -e MYSQL_ROOT_PASSWORD=wolkenkit -e MYSQL_USER=wolkenkit -e MYSQL_PASSWORD=wolkenkit -e MYSQL_DATABASE=wolkenkit --name mariadb-units mariadb:10.3.5 --bind-address=0.0.0.0');
      shell.exec('docker run -d -p 27018:27017 --name mongodb-units mongo:3.4.2');
      shell.exec('docker run -d -p 3310:3306 -e MYSQL_ROOT_PASSWORD=wolkenkit -e MYSQL_USER=wolkenkit -e MYSQL_PASSWORD=wolkenkit -e MYSQL_DATABASE=wolkenkit --name mysql-units mysql:5.7.21 --bind-address=0.0.0.0');
      shell.exec('docker run -d -p 5433:5432 -e POSTGRES_USER=wolkenkit -e POSTGRES_PASSWORD=wolkenkit -e POSTGRES_DB=wolkenkit --name postgres-units postgres:9.6.4-alpine');

      await waitForMaria({ url: env.MARIA_URL_UNITS });
      await waitForMongo({ url: env.MONGO_URL_UNITS });
      await waitForMysql({ url: env.MYSQL_URL_UNITS });
      await waitForPostgres({ url: env.POSTGRES_URL_UNITS });
    } catch (ex) {
      return done(ex);
    }
    done();
  })();
};

module.exports = pre;
