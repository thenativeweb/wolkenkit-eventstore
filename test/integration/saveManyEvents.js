'use strict';

const Event = require('commands-events').Event,
      flaschenpost = require('flaschenpost'),
      processenv = require('processenv'),
      uuid = require('uuidv4');

const batchCount = processenv('BATCH_COUNT'),
      batchSize = processenv('BATCH_SIZE'),
      namespace = processenv('NAMESPACE'),
      type = processenv('TYPE'),
      url = processenv('URL');

const sparbuch = require(`../../${type}`);

const logger = flaschenpost.getLogger();

const saveEventBatch = function (remaining) {
  if (remaining <= 0) {
    /* eslint-disable no-process-exit */
    process.exit(0);
    /* eslint-enable no-process-exit */
  }

  const events = [];

  for (let i = 0; i < batchSize; i++) {
    const event = new Event({
      context: { name: 'planning' },
      aggregate: { name: 'peerGroup', id: uuid() },
      name: 'started',
      data: { initiator: 'Jane Doe', destination: 'Riva' },
      metadata: { correlationId: uuid(), causationId: uuid() }
    });

    event.metadata.revision = 1;

    events.push(event);
  }

  sparbuch.saveEvents({ events }, err => {
    if (err) {
      logger.error('Failed to save events.', { err });
      /* eslint-disable no-process-exit */
      process.exit(1);
      /* eslint-enable no-process-exit */
    }
    saveEventBatch(remaining - 1);
  });
};

sparbuch.initialize({ url, namespace }, err => {
  if (err) {
    logger.error('Failed to initialize sparbuch.', { type, err });
    /* eslint-disable no-process-exit */
    process.exit(1);
    /* eslint-enable no-process-exit */
  }

  saveEventBatch(batchCount);
});
