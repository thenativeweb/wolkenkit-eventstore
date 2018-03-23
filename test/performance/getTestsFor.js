'use strict';

const assert = require('assertthat'),
      { Event } = require('commands-events'),
      measureTime = require('measure-time'),
      uuid = require('uuidv4');

const getEventsForAggregateId = function ({ aggregateId, batchCount, batchSize }) {
  if (!aggregateId) {
    throw new Error('Aggregate id is missing.');
  }
  if (!batchCount) {
    throw new Error('Batch count is missing.');
  }
  if (!batchSize) {
    throw new Error('Batch size is missing.');
  }

  const batches = [];

  for (let i = 0; i < batchCount; i++) {
    batches[i] = [];

    for (let j = 0; j < batchSize; j++) {
      const event = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: aggregateId },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: aggregateId, causationId: aggregateId }
      });

      event.metadata.revision = i * batchSize + j + 1;

      batches[i].push(event);
    }
  }

  return batches;
};

/* eslint-disable mocha/max-top-level-suites */
const getTestsFor = function (Sparbuch, { url, type }) {
  let namespace,
      sparbuch;

  setup(async () => {
    namespace = uuid();
    sparbuch = new Sparbuch();

    await sparbuch.initialize({ url, namespace });
  });

  teardown(async () => {
    await sparbuch.destroy();
  });

  suite('saveEvents', () => {
    test('1000 events individually.', async () => {
      const expected = {
        mongodb: 4,
        postgres: 4,
        inmemory: 4
      };

      const batches = getEventsForAggregateId({
        aggregateId: uuid(),
        batchCount: 1000,
        batchSize: 1
      });

      const getElapsed = measureTime();

      for (let i = 0; i < batches.length; i++) {
        const events = batches[i];

        await sparbuch.saveEvents({ events });
      }

      const elapsed = getElapsed();

      assert.that(elapsed.millisecondsTotal).is.lessThan(expected[type] * 1000);
    });

    test('10000 events individually.', async () => {
      const expected = {
        mongodb: 16,
        postgres: 16,
        inmemory: 16
      };

      const batches = getEventsForAggregateId({
        aggregateId: uuid(),
        batchCount: 10000,
        batchSize: 1
      });

      const getElapsed = measureTime();

      for (let i = 0; i < batches.length; i++) {
        const events = batches[i];

        await sparbuch.saveEvents({ events });
      }

      const elapsed = getElapsed();

      assert.that(elapsed.millisecondsTotal).is.lessThan(expected[type] * 1000);
    });

    test('10000 events in batches of 10.', async () => {
      const expected = {
        mongodb: 16,
        postgres: 16,
        inmemory: 16
      };

      const batches = getEventsForAggregateId({
        aggregateId: uuid(),
        batchCount: 1000,
        batchSize: 10
      });

      const getElapsed = measureTime();

      for (let i = 0; i < batches.length; i++) {
        const events = batches[i];

        await sparbuch.saveEvents({ events });
      }

      const elapsed = getElapsed();

      assert.that(elapsed.millisecondsTotal).is.lessThan(expected[type] * 1000);
    });
  });

  suite('getEventStream', () => {
    test('1000 events.', async () => {
      const expected = {
        mongodb: 2,
        postgres: 2,
        inmemory: 2
      };

      const aggregateId = uuid();

      const batches = getEventsForAggregateId({
        aggregateId,
        batchCount: 10,
        batchSize: 100
      });

      for (let i = 0; i < batches.length; i++) {
        const events = batches[i];

        await sparbuch.saveEvents({ events });
      }

      const getElapsed = measureTime();

      const eventStream = await sparbuch.getEventStream(aggregateId);

      await new Promise((resolve, reject) => {
        eventStream.once('end', () => {
          try {
            const elapsed = getElapsed();

            assert.that(elapsed.millisecondsTotal).is.lessThan(expected[type] * 1000);
          } catch (ex) {
            return reject(ex);
          }
          resolve();
        });

        eventStream.resume();
      });
    });

    test('10000 events.', async () => {
      const expected = {
        mongodb: 16,
        postgres: 16,
        inmemory: 16
      };

      const aggregateId = uuid();

      const batches = getEventsForAggregateId({
        aggregateId,
        batchCount: 100,
        batchSize: 100
      });

      for (let i = 0; i < batches.length; i++) {
        const events = batches[i];

        await sparbuch.saveEvents({ events });
      }

      const getElapsed = measureTime();

      const eventStream = await sparbuch.getEventStream(aggregateId);

      await new Promise((resolve, reject) => {
        eventStream.once('end', () => {
          try {
            const elapsed = getElapsed();

            assert.that(elapsed.millisecondsTotal).is.lessThan(expected[type] * 1000);
          } catch (ex) {
            return reject(ex);
          }
          resolve();
        });

        eventStream.resume();
      });
    });
  });
};
/* eslint-enable mocha/max-top-level-suites */

module.exports = getTestsFor;
