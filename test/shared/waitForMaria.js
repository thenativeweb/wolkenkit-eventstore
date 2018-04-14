'use strict';

const { parse } = require('pg-connection-string'),
      mysql = require('mysql2/promise'),
      retry = require('async-retry');

const waitForMaria = async function ({ url }) {
  if (!url) {
    throw new Error('Url is missing.');
  }

  const { host, port, user, password, database } = parse(url);

  const pool = mysql.createPool({
    host,
    port,
    user,
    password,
    database
  });

  await retry(async () => {
    const connection = await pool.getConnection();

    await connection.release();
  });

  await pool.end();
};

module.exports = waitForMaria;
