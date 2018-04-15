'use strict';

const { parse } = require('pg-connection-string'),
      pg = require('pg'),
      retry = require('async-retry');

const waitForPostgres = async function ({ url }) {
  if (!url) {
    throw new Error('Url is missing.');
  }

  const pool = new pg.Pool(parse(url));

  await retry(async () => {
    const database = await pool.connect();

    database.release();
  });

  await pool.end();
};

module.exports = waitForPostgres;
