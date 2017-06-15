'use strict';

const assert = require('assertthat'),
      async = require('async'),
      Event = require('commands-events').Event,
      measureTime = require('measure-time'),
      uuid = require('uuidv4');

const getEventsForAggregateId = function (options) {
  if (!options) {
    throw new Error('Options are missing.');
  }
  if (!options.aggregateId) {
    throw new Error('Aggregate id is missing.');
  }
  if (!options.batchCount) {
    throw new Error('Batch count is missing.');
  }
  if (!options.batchSize) {
    throw new Error('Batch size is missing.');
  }

  const { aggregateId, batchCount, batchSize } = options;

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
const getTestsFor = function (Sparbuch, options) {
  let namespace,
      sparbuch;

  setup(done => {
    namespace = uuid();
    sparbuch = new Sparbuch();

    sparbuch.initialize({ url: options.url, namespace }, done);
  });

  teardown(done => {
    sparbuch.destroy(done);
  });

  suite('saveEvents', () => {
    test('1000 events individually.', done => {
      const expected = {
        mongodb: 1.5,
        postgres: 1.5
      };

      const batches = getEventsForAggregateId({
        aggregateId: uuid(),
        batchCount: 1000,
        batchSize: 1
      });

      const getElapsed = measureTime();

      async.eachSeries(batches, (events, callback) => {
        sparbuch.saveEvents({ events }, callback);
      }, err => {
        const elapsed = getElapsed();

        assert.that(err).is.null();
        assert.that(elapsed.millisecondsTotal).is.lessThan(expected[options.type] * 1000);
        done();
      });
    });

    test('10000 events individually.', done => {
      const expected = {
        mongodb: 15,
        postgres: 7
      };

      const batches = getEventsForAggregateId({
        aggregateId: uuid(),
        batchCount: 10000,
        batchSize: 1
      });

      const getElapsed = measureTime();

      async.eachSeries(batches, (events, callback) => {
        sparbuch.saveEvents({ events }, callback);
      }, err => {
        const elapsed = getElapsed();

        assert.that(err).is.null();
        assert.that(elapsed.millisecondsTotal).is.lessThan(expected[options.type] * 1000);
        done();
      });
    });

    test('10000 events in batches of 10.', done => {
      const expected = {
        mongodb: 10,
        postgres: 2
      };

      const batches = getEventsForAggregateId({
        aggregateId: uuid(),
        batchCount: 1000,
        batchSize: 10
      });

      const getElapsed = measureTime();

      async.eachSeries(batches, (events, callback) => {
        sparbuch.saveEvents({ events }, callback);
      }, err => {
        const elapsed = getElapsed();

        assert.that(err).is.null();
        assert.that(elapsed.millisecondsTotal).is.lessThan(expected[options.type] * 1000);
        done();
      });
    });
  });

  suite('getEventStream', () => {
    test('1000 events.', done => {
      const expected = {
        mongodb: 1,
        postgres: 0.25
      };

      const aggregateId = uuid();

      const batches = getEventsForAggregateId({
        aggregateId,
        batchCount: 10,
        batchSize: 100
      });

      async.eachSeries(batches, (events, callback) => {
        sparbuch.saveEvents({ events }, callback);
      }, errSaveEvents => {
        assert.that(errSaveEvents).is.null();

        const getElapsed = measureTime();

        sparbuch.getEventStream(aggregateId, (errGetEventStream, eventStream) => {
          assert.that(errGetEventStream).is.null();

          eventStream.once('end', () => {
            const elapsed = getElapsed();

            assert.that(elapsed.millisecondsTotal).is.lessThan(expected[options.type] * 1000);
            done();
          });

          eventStream.resume();
        });
      });
    });

    test('10000 events.', done => {
      const expected = {
        mongodb: 10,
        postgres: 2
      };

      const aggregateId = uuid();

      const batches = getEventsForAggregateId({
        aggregateId,
        batchCount: 100,
        batchSize: 100
      });

      async.eachSeries(batches, (events, callback) => {
        sparbuch.saveEvents({ events }, callback);
      }, errSaveEvents => {
        assert.that(errSaveEvents).is.null();

        const getElapsed = measureTime();

        sparbuch.getEventStream(aggregateId, (errGetEventStream, eventStream) => {
          assert.that(errGetEventStream).is.null();

          eventStream.once('end', () => {
            const elapsed = getElapsed();

            assert.that(elapsed.millisecondsTotal).is.lessThan(expected[options.type] * 1000);
            done();
          });

          eventStream.resume();
        });
      });
    });
  });
};
/* eslint-enable mocha/max-top-level-suites */

module.exports = getTestsFor;
