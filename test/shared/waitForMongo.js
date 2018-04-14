'use strict';

const { MongoClient } = require('mongodb'),
      retry = require('async-retry');

const waitForMongo = async function ({ url }) {
  if (!url) {
    throw new Error('Url is missing.');
  }

  await retry(async () => {
    /* eslint-disable id-length */
    const client = await MongoClient.connect(url, { w: 1 });
    /* eslint-enable id-length */

    await client.close();
  });
};

module.exports = waitForMongo;
