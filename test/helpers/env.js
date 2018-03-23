'use strict';

/* eslint-disable no-process-env */
const env = {
  MONGO_URL_UNITS: process.env.MONGO_URL_UNITS || 'mongodb://local.wolkenkit.io:27018/wolkenkit',
  MONGO_URL_INTEGRATION: process.env.MONGO_URL_INTEGRATION || 'mongodb://local.wolkenkit.io:27019/wolkenkit',
  MONGO_URL_PERFORMANCE: process.env.MONGO_URL_PERFORMANCE || 'mongodb://local.wolkenkit.io:27020/wolkenkit',
  POSTGRES_URL_UNITS: process.env.POSTGRES_URL_UNITS || 'pg://wolkenkit:wolkenkit@local.wolkenkit.io:5433/wolkenkit',
  POSTGRES_URL_INTEGRATION: process.env.POSTGRES_URL_INTEGRATION || 'pg://wolkenkit:wolkenkit@local.wolkenkit.io:5434/wolkenkit',
  POSTGRES_URL_PERFORMANCE: process.env.POSTGRES_URL_PERFORMANCE || 'pg://wolkenkit:wolkenkit@local.wolkenkit.io:5435/wolkenkit',
  INMEMORY_URL_UNITS: process.env.INMEMORY_URL_UNITS || '/dev/null',
  INMEMORY_URL_INTEGRATION: process.env.INMEMORY_URL_INTEGRATION || '/dev/null',
  INMEMORY_URL_PERFORMANCE: process.env.INMEMORY_URL_PERFORMANCE || '/dev/null'
};
/* eslint-enable no-process-env */

module.exports = env;
