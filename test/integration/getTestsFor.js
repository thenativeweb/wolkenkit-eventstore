'use strict';

const path = require('path');

const uuid = require('uuidv4');

const runApp = require('../shared/runApp');

const runAppConcurrently = async function ({ app, count, env }) {
  const apps = [];

  for (let i = 0; i < count; i++) {
    apps.push(runApp({ app, env }));
  }

  await Promise.all(apps);
};

const getTestsFor = function ({ type, url }) {
  suite('saves concurrently', () => {
    test('2 clients, 5 batches, 1 event each.', async () => {
      const app = path.join(__dirname, 'saveManyEvents.js');

      const env = {
        BATCH_COUNT: 5,
        BATCH_SIZE: 1,
        FLASCHENPOST_FORMATTER: 'human',
        NAMESPACE: `store_${uuid()}`,
        TYPE: type,
        URL: url
      };

      await runAppConcurrently({ app, count: 2, env });
    });

    test('20 clients, 10 batches, 10 events each.', async () => {
      const app = path.join(__dirname, 'saveManyEvents.js');

      const env = {
        BATCH_COUNT: 10,
        BATCH_SIZE: 10,
        FLASCHENPOST_FORMATTER: 'human',
        NAMESPACE: `store_${uuid()}`,
        TYPE: type,
        URL: url
      };

      await runAppConcurrently({ app, count: 20, env });
    });
  });
};

module.exports = getTestsFor;
