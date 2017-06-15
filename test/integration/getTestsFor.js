'use strict';

const path = require('path');

const assert = require('assertthat'),
      async = require('async'),
      uuid = require('uuidv4');

const runApp = require('../helpers/runApp');

const runAppConcurrently = function (options, callback) {
  const apps = [];

  for (let i = 0; i < options.count; i++) {
    apps.push(done => runApp({ app: options.app, env: options.env }, done));
  }

  async.parallel(apps, callback);
};

const getTestsFor = function (options) {
  suite('saves concurrently', () => {
    test('2 clients, 5 batches, 1 event each.', done => {
      const app = path.join(__dirname, 'saveManyEvents.js');

      const env = {
        BATCH_COUNT: 5,
        BATCH_SIZE: 1,
        FLASCHENPOST_FORMATTER: 'human',
        NAMESPACE: `store_${uuid()}`,
        TYPE: options.type,
        URL: options.url
      };

      runAppConcurrently({ app, count: 2, env }, err => {
        assert.that(err).is.null();
        done();
      });
    });

    test('20 clients, 10 batches, 10 events each.', done => {
      const app = path.join(__dirname, 'saveManyEvents.js');

      const env = {
        BATCH_COUNT: 10,
        BATCH_SIZE: 10,
        FLASCHENPOST_FORMATTER: 'human',
        NAMESPACE: `store_${uuid()}`,
        TYPE: options.type,
        URL: options.url
      };

      runAppConcurrently({ app, count: 20, env }, err => {
        assert.that(err).is.null();
        done();
      });
    });
  });
};

module.exports = getTestsFor;
