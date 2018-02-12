'use strict';

const { Event } = require('commands-events'),
      flaschenpost = require('flaschenpost'),
      processenv = require('processenv'),
      uuid = require('uuidv4');

const batchCount = processenv('BATCH_COUNT'),
      batchSize = processenv('BATCH_SIZE'),
      namespace = processenv('NAMESPACE'),
      type = processenv('TYPE'),
      url = processenv('URL');

const Sparbuch = require(`../../lib/${type}/Sparbuch`);

const sparbuch = new Sparbuch();
const logger = flaschenpost.getLogger();

const saveEventBatch = async function (remaining) {
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

  try {
    await sparbuch.saveEvents({ events });
  } catch (ex) {
    logger.error('Failed to save events.', { ex });
    /* eslint-disable no-process-exit */
    process.exit(1);
    /* eslint-enable no-process-exit */
  }

  await saveEventBatch(remaining - 1);
};

(async () => {
  try {
    await sparbuch.initialize({ url, namespace });
  } catch (ex) {
    logger.error('Failed to initialize sparbuch.', { type, ex });
    /* eslint-disable no-process-exit */
    process.exit(1);
    /* eslint-enable no-process-exit */
  }

  await saveEventBatch(batchCount);
})();
