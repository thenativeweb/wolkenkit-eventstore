'use strict';

/* eslint-disable no-process-env */
const env = {
  MARIA_URL_UNITS: process.env.MARIA_URL_UNITS || 'mariadb://wolkenkit:wolkenkit@local.wolkenkit.io:3307/wolkenkit',
  MARIA_URL_INTEGRATION: process.env.MARIA_URL_INTEGRATION || 'mariadb://wolkenkit:wolkenkit@local.wolkenkit.io:3308/wolkenkit',
  MARIA_URL_PERFORMANCE: process.env.MARIA_URL_PERFORMANCE || 'mariadb://wolkenkit:wolkenkit@local.wolkenkit.io:3309/wolkenkit',
  MONGO_URL_UNITS: process.env.MONGO_URL_UNITS || 'mongodb://wolkenkit:wolkenkit@local.wolkenkit.io:27018/wolkenkit',
  MONGO_URL_INTEGRATION: process.env.MONGO_URL_INTEGRATION || 'mongodb://wolkenkit:wolkenkit@local.wolkenkit.io:27019/wolkenkit',
  MONGO_URL_PERFORMANCE: process.env.MONGO_URL_PERFORMANCE || 'mongodb://wolkenkit:wolkenkit@local.wolkenkit.io:27020/wolkenkit',
  MYSQL_URL_UNITS: process.env.MYSQL_URL_UNITS || 'mysql://wolkenkit:wolkenkit@local.wolkenkit.io:3310/wolkenkit',
  MYSQL_URL_INTEGRATION: process.env.MYSQL_URL_INTEGRATION || 'mysql://wolkenkit:wolkenkit@local.wolkenkit.io:3311/wolkenkit',
  MYSQL_URL_PERFORMANCE: process.env.MYSQL_URL_PERFORMANCE || 'mysql://wolkenkit:wolkenkit@local.wolkenkit.io:3312/wolkenkit',
  POSTGRES_URL_UNITS: process.env.POSTGRES_URL_UNITS || 'pg://wolkenkit:wolkenkit@local.wolkenkit.io:5433/wolkenkit',
  POSTGRES_URL_INTEGRATION: process.env.POSTGRES_URL_INTEGRATION || 'pg://wolkenkit:wolkenkit@local.wolkenkit.io:5434/wolkenkit',
  POSTGRES_URL_PERFORMANCE: process.env.POSTGRES_URL_PERFORMANCE || 'pg://wolkenkit:wolkenkit@local.wolkenkit.io:5435/wolkenkit',
  SQLSERVER_URL_UNITS: process.env.SQLSERVER_URL_UNITS || 'mssql://SA:Wolkenkit123@local.wolkenkit.io:1433/wolkenkit',
  SQLSERVER_URL_INTEGRATION: process.env.SQLSERVER_URL_UNITS || 'mssql://SA:Wolkenkit123@local.wolkenkit.io:1434/wolkenkit',
  SQLSERVER_URL_PERFORMANCE: process.env.SQLSERVER_URL_UNITS || 'mssql://SA:Wolkenkit123@local.wolkenkit.io:1435/wolkenkit'
};
/* eslint-enable no-process-env */

module.exports = env;
