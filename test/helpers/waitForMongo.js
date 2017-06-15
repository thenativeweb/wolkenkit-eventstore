'use strict';

const MongoClient = require('mongodb').MongoClient,
      retry = require('retry');

const waitForMongo = function (options, callback) {
  if (!options) {
    throw new Error('Options are missing.');
  }
  if (!options.url) {
    throw new Error('Url is missing.');
  }

  const { url } = options;

  const operation = retry.operation();

  operation.attempt(() => {
    /* eslint-disable id-length */
    MongoClient.connect(url, { w: 1 }, (err, db) => {
      /* eslint-enable id-length */
      if (operation.retry(err)) {
        return;
      }

      if (err) {
        return callback(operation.mainError());
      }

      db.close(errClose => {
        if (errClose) {
          return callback(errClose);
        }

        callback(null);
      });
    });
  });
};

module.exports = waitForMongo;
