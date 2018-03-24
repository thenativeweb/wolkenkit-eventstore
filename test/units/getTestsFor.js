'use strict';

const { EventEmitter } = require('events');

const assert = require('assertthat'),
      { Event } = require('commands-events'),
      toArray = require('streamtoarray'),
      uuid = require('uuidv4');

/* eslint-disable mocha/max-top-level-suites */
const getTestsFor = function (Sparbuch, { url, type, nonExistentUrl, startContainer, stopContainer }) {
  let namespace,
      sparbuch;

  setup(() => {
    sparbuch = new Sparbuch();
    namespace = uuid();
  });

  teardown(async () => {
    await sparbuch.destroy();
  });

  test('is a function.', async () => {
    assert.that(Sparbuch).is.ofType('function');
  });

  test('is an event emitter.', async () => {
    assert.that(sparbuch).is.instanceOf(EventEmitter);
  });

  if (type !== 'inmemory') {
    test('emits a disconnect event when the connection to the database becomes lost.', async function () {
      this.timeout(15 * 1000);

      await sparbuch.initialize({ url, namespace });

      await new Promise(async (resolve, reject) => {
        sparbuch.once('disconnect', async () => {
          try {
            await startContainer();
          } catch (ex) {
            return reject(ex);
          }
          resolve();
        });

        try {
          await stopContainer();
        } catch (ex) {
          reject(ex);
        }
      });
    });
  }

  suite('initialize', () => {
    test('is a function.', async () => {
      assert.that(sparbuch.initialize).is.ofType('function');
    });

    test('throws an error if url is missing.', async () => {
      await assert.that(async () => {
        await sparbuch.initialize({});
      }).is.throwingAsync('Url is missing.');
    });

    test('throws an error if namespace is missing.', async () => {
      await assert.that(async () => {
        await sparbuch.initialize({ url });
      }).is.throwingAsync('Namespace is missing.');
    });

    if (type !== 'inmemory') {
      test('returns an error if the database is not reachable.', async () => {
        await assert.that(async () => {
          await sparbuch.initialize({ url: nonExistentUrl, namespace });
        }).is.throwingAsync();
      });
    }

    test('does not throw an error if the database is reachable.', async () => {
      await assert.that(async () => {
        await sparbuch.initialize({ url, namespace });
      }).is.not.throwingAsync();
    });

    test('does not throw an error if tables, indexes & co. do already exist.', async () => {
      await assert.that(async () => {
        await sparbuch.initialize({ url, namespace });
        await sparbuch.initialize({ url, namespace });
      }).is.not.throwingAsync();
    });

    test('throws an error if the aggregate id and revision of the new event are already in use.', async () => {
      const event = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      event.metadata.revision = 1;

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveEvents({ events: event });

      await assert.that(async () => {
        await sparbuch.saveEvents({ events: event });
      }).is.throwingAsync('Aggregate id and revision already exist.');
    });

    suite('event stream order', () => {
      test('assigns the position 1 to the first event.', async () => {
        const event = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: uuid() },
          name: 'started',
          data: { initiator: 'Jane Doe', destination: 'Riva' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        event.metadata.revision = 1;

        await sparbuch.initialize({ url, namespace });
        await sparbuch.saveEvents({ events: event });

        const eventStream = await sparbuch.getEventStream(event.aggregate.id);
        const aggregateEvents = await toArray(eventStream);

        assert.that(aggregateEvents.length).is.equalTo(1);
        assert.that(aggregateEvents[0].metadata.position).is.equalTo(1);
      });

      test('assigns increasing positions to subsequent events.', async () => {
        const eventStarted = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: uuid() },
          name: 'started',
          data: { initiator: 'Jane Doe', destination: 'Riva' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        const eventJoined = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: eventStarted.aggregate.id },
          name: 'joined',
          data: { participant: 'Jane Doe' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        eventStarted.metadata.revision = 1;
        eventJoined.metadata.revision = 2;

        await sparbuch.initialize({ url, namespace });
        await sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]});

        const eventStream = await sparbuch.getEventStream(eventStarted.aggregate.id);
        const aggregateEvents = await toArray(eventStream);

        assert.that(aggregateEvents.length).is.equalTo(2);
        assert.that(aggregateEvents[0].metadata.position).is.equalTo(1);
        assert.that(aggregateEvents[1].metadata.position).is.equalTo(2);
      });

      test('assigns increasing positions even when saving the events individually.', async () => {
        const eventStarted = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: uuid() },
          name: 'started',
          data: { initiator: 'Jane Doe', destination: 'Riva' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        const eventJoined = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: eventStarted.aggregate.id },
          name: 'joined',
          data: { participant: 'Jane Doe' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        eventStarted.metadata.revision = 1;
        eventJoined.metadata.revision = 2;

        await sparbuch.initialize({ url, namespace });
        await sparbuch.saveEvents({ events: eventStarted });
        await sparbuch.saveEvents({ events: eventJoined });

        const eventStream = await sparbuch.getEventStream(eventStarted.aggregate.id);
        const aggregateEvents = await toArray(eventStream);

        assert.that(aggregateEvents.length).is.equalTo(2);
        assert.that(aggregateEvents[0].metadata.position).is.equalTo(1);
        assert.that(aggregateEvents[1].metadata.position).is.equalTo(2);
      });

      test('ensures that positions are unique across aggregates.', async () => {
        const eventStarted1 = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: uuid() },
          name: 'started',
          data: { initiator: 'Jane Doe', destination: 'Riva' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        const eventStarted2 = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: uuid() },
          name: 'started',
          data: { initiator: 'Jane Doe', destination: 'Riva' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        eventStarted1.metadata.revision = 1;
        eventStarted2.metadata.revision = 1;

        await sparbuch.initialize({ url, namespace });
        await sparbuch.saveEvents({ events: eventStarted1 });
        await sparbuch.saveEvents({ events: eventStarted2 });

        const eventStream1 = await sparbuch.getEventStream(eventStarted1.aggregate.id);
        const aggregateEvents1 = await toArray(eventStream1);

        assert.that(aggregateEvents1.length).is.equalTo(1);
        assert.that(aggregateEvents1[0].metadata.position).is.equalTo(1);

        const eventStream2 = await sparbuch.getEventStream(eventStarted2.aggregate.id);
        const aggregateEvents2 = await toArray(eventStream2);

        assert.that(aggregateEvents2.length).is.equalTo(1);
        assert.that(aggregateEvents2[0].metadata.position).is.equalTo(2);
      });

      test('returns the saved events enriched by their positions.', async () => {
        const event = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: uuid() },
          name: 'started',
          data: { initiator: 'Jane Doe', destination: 'Riva' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        event.metadata.revision = 1;

        await sparbuch.initialize({ url, namespace });
        const savedEvents = await sparbuch.saveEvents({ events: event });

        assert.that(savedEvents.length).is.equalTo(1);
        assert.that(savedEvents[0].metadata.position).is.equalTo(1);
      });

      test('does not change the events that were given as arguments.', async () => {
        const event = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: uuid() },
          name: 'started',
          data: { initiator: 'Jane Doe', destination: 'Riva' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        event.metadata.revision = 1;

        await sparbuch.initialize({ url, namespace });
        await sparbuch.saveEvents({ events: event });

        assert.that(event.metadata.position).is.undefined();
      });
    });
  });

  suite('getLastEvent', () => {
    test('is a function.', async () => {
      assert.that(sparbuch.getLastEvent).is.ofType('function');
    });

    test('throws an error if aggregate id is missing.', async () => {
      await assert.that(async () => {
        await sparbuch.getLastEvent();
      }).is.throwingAsync('Aggregate id is missing.');
    });

    test('returns undefined for an aggregate without events.', async () => {
      await sparbuch.initialize({ url, namespace });

      const event = await sparbuch.getLastEvent(uuid());

      assert.that(event).is.undefined();
    });

    test('returns the last event for the given aggregate.', async () => {
      const aggregateId = uuid();

      const eventStarted = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: aggregateId },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      const eventJoined = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: eventStarted.aggregate.id },
        name: 'joined',
        data: { participant: 'Jane Doe' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      eventStarted.metadata.revision = 1;
      eventJoined.metadata.revision = 2;

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]});

      const event = await sparbuch.getLastEvent(aggregateId);

      assert.that(event.name).is.equalTo('joined');
      assert.that(event.metadata.revision).is.equalTo(2);
    });

    test('correctly handles null, undefined and empty arrays.', async () => {
      const aggregateId = uuid();

      const eventJoined = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: aggregateId },
        name: 'joined',
        data: {
          initiator: null,
          destination: undefined,
          participants: []
        },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      eventJoined.metadata.revision = 1;

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveEvents({ events: [ eventJoined ]});

      const event = await sparbuch.getLastEvent(aggregateId);

      assert.that(event.data.initiator).is.null();
      assert.that(event.data.participants).is.equalTo([]);
    });
  });

  suite('getEventStream', () => {
    test('is a function.', async () => {
      assert.that(sparbuch.getEventStream).is.ofType('function');
    });

    test('throws an error if aggregate id is missing.', async () => {
      await assert.that(async () => {
        await sparbuch.getEventStream();
      }).is.throwingAsync('Aggregate id is missing.');
    });

    test('throws an error if from revision is greater than to revision.', async () => {
      await assert.that(async () => {
        await sparbuch.getEventStream(uuid(), { fromRevision: 42, toRevision: 23 });
      }).is.throwingAsync('From revision is greater than to revision.');
    });

    test('returns an empty stream for a non-existent aggregate.', async () => {
      await sparbuch.initialize({ url, namespace });

      const eventStream = await sparbuch.getEventStream(uuid());
      const aggregateEvents = await toArray(eventStream);

      assert.that(aggregateEvents.length).is.equalTo(0);
    });

    test('returns a stream of events for the given aggregate.', async () => {
      const eventStarted = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      const eventJoined = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: eventStarted.aggregate.id },
        name: 'joined',
        data: { participant: 'Jane Doe' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      eventStarted.metadata.revision = 1;
      eventJoined.metadata.revision = 2;

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]});

      const eventStream = await sparbuch.getEventStream(eventStarted.aggregate.id);
      const aggregateEvents = await toArray(eventStream);

      assert.that(aggregateEvents.length).is.equalTo(2);
      assert.that(aggregateEvents[0].name).is.equalTo('started');
      assert.that(aggregateEvents[1].name).is.equalTo('joined');
    });

    test('returns a stream from revision.', async () => {
      const eventStarted = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      const eventJoined = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: eventStarted.aggregate.id },
        name: 'joined',
        data: { participant: 'Jane Doe' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      eventStarted.metadata.revision = 1;
      eventJoined.metadata.revision = 2;

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]});

      const eventStream = await sparbuch.getEventStream(eventStarted.aggregate.id, { fromRevision: 2 });
      const aggregateEvents = await toArray(eventStream);

      assert.that(aggregateEvents.length).is.equalTo(1);
      assert.that(aggregateEvents[0].name).is.equalTo('joined');
    });

    test('returns a stream to revision.', async () => {
      const eventStarted = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      const eventJoined = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: eventStarted.aggregate.id },
        name: 'joined',
        data: { participant: 'Jane Doe' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      eventStarted.metadata.revision = 1;
      eventJoined.metadata.revision = 2;

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]});

      const eventStream = await sparbuch.getEventStream(eventStarted.aggregate.id, { toRevision: 1 });
      const aggregateEvents = await toArray(eventStream);

      assert.that(aggregateEvents.length).is.equalTo(1);
      assert.that(aggregateEvents[0].name).is.equalTo('started');
    });
  });

  suite('getUnpublishedEventStream', () => {
    test('is a function.', async () => {
      assert.that(sparbuch.getUnpublishedEventStream).is.ofType('function');
    });

    test('returns an empty stream if there are no unpublished events.', async () => {
      await sparbuch.initialize({ url, namespace });

      const eventStream = await sparbuch.getUnpublishedEventStream();
      const unpublishedEvents = await toArray(eventStream);

      assert.that(unpublishedEvents.length).is.equalTo(0);
    });

    test('returns a stream of unpublished events.', async () => {
      const eventStarted = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      const eventJoined = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: eventStarted.aggregate.id },
        name: 'joined',
        data: { participant: 'Jane Doe' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      eventStarted.metadata.revision = 1;
      eventStarted.metadata.published = true;
      eventJoined.metadata.revision = 2;

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]});

      const eventStream = await sparbuch.getUnpublishedEventStream();
      const unpublishedEvents = await toArray(eventStream);

      assert.that(unpublishedEvents.length).is.equalTo(1);
      assert.that(unpublishedEvents[0].name).is.equalTo('joined');
    });
  });

  suite('saveEvents', () => {
    test('is a function.', async () => {
      assert.that(sparbuch.saveEvents).is.ofType('function');
    });

    test('throws an error if events are missing.', async () => {
      await assert.that(async () => {
        await sparbuch.saveEvents({});
      }).is.throwingAsync('Events are missing.');
    });

    test('saves a single event.', async () => {
      const event = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      event.metadata.revision = 1;

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveEvents({ events: event });

      const eventStream = await sparbuch.getEventStream(event.aggregate.id);
      const aggregateEvents = await toArray(eventStream);

      assert.that(aggregateEvents.length).is.equalTo(1);
      assert.that(aggregateEvents[0].name).is.equalTo('started');
    });

    test('saves multiple events.', async () => {
      const eventStarted = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      const eventJoined = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: eventStarted.aggregate.id },
        name: 'joined',
        data: { participant: 'Jane Doe' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      eventStarted.metadata.revision = 1;
      eventJoined.metadata.revision = 2;

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]});

      const eventStream = await sparbuch.getEventStream(eventStarted.aggregate.id);
      const aggregateEvents = await toArray(eventStream);

      assert.that(aggregateEvents.length).is.equalTo(2);
      assert.that(aggregateEvents[0].name).is.equalTo('started');
      assert.that(aggregateEvents[1].name).is.equalTo('joined');
    });

    test('correctly handles undefined and null.', async () => {
      const event = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: null, destination: undefined },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      event.metadata.revision = 1;

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveEvents({ events: event });

      const eventStream = await sparbuch.getEventStream(event.aggregate.id);
      const aggregateEvents = await toArray(eventStream);

      assert.that(aggregateEvents.length).is.equalTo(1);
      assert.that(aggregateEvents[0].data).is.equalTo({ initiator: null });
    });
  });

  suite('markEventsAsPublished', () => {
    test('is a function.', async () => {
      assert.that(sparbuch.markEventsAsPublished).is.ofType('function');
    });

    test('throws an error if aggregate id is missing.', async () => {
      await assert.that(async () => {
        await sparbuch.markEventsAsPublished({});
      }).is.throwingAsync('Aggregate id is missing.');
    });

    test('throws an error if from revision is missing.', async () => {
      await assert.that(async () => {
        await sparbuch.markEventsAsPublished({ aggregateId: uuid() });
      }).is.throwingAsync('From revision is missing.');
    });

    test('throws an error if to revision is missing.', async () => {
      await assert.that(async () => {
        await sparbuch.markEventsAsPublished({ aggregateId: uuid(), fromRevision: 5 });
      }).is.throwingAsync('To revision is missing.');
    });

    test('marks the specified events as published.', async () => {
      const eventStarted = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      const eventJoinedFirst = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: eventStarted.aggregate.id },
        name: 'joined',
        data: { participant: 'Jane Doe' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      const eventJoinedSecond = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: eventStarted.aggregate.id },
        name: 'joined',
        data: { participant: 'Jennifer Doe' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      eventStarted.metadata.revision = 1;
      eventJoinedFirst.metadata.revision = 2;
      eventJoinedSecond.metadata.revision = 3;

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveEvents({ events: [ eventStarted, eventJoinedFirst, eventJoinedSecond ]});

      await sparbuch.markEventsAsPublished({
        aggregateId: eventStarted.aggregate.id,
        fromRevision: 1,
        toRevision: 2
      });

      const eventStream = await sparbuch.getEventStream(eventStarted.aggregate.id);
      const aggregateEvents = await toArray(eventStream);

      assert.that(aggregateEvents[0].metadata.published).is.true();
      assert.that(aggregateEvents[1].metadata.published).is.true();
      assert.that(aggregateEvents[2].metadata.published).is.false();
    });
  });

  suite('getSnapshot', () => {
    test('is a function.', async () => {
      assert.that(sparbuch.getSnapshot).is.ofType('function');
    });

    test('throws an error if aggregate id is missing.', async () => {
      await assert.that(async () => {
        await sparbuch.getSnapshot();
      }).is.throwingAsync('Aggregate id is missing.');
    });

    test('returns undefined for an aggregate without a snapshot.', async () => {
      await sparbuch.initialize({ url, namespace });

      const snapshot = await sparbuch.getSnapshot(uuid());

      assert.that(snapshot).is.undefined();
    });

    test('returns a snapshot for the given aggregate.', async () => {
      const aggregateId = uuid();

      const state = {
        initiator: 'Jane Doe',
        destination: 'Riva',
        participants: [ 'Jane Doe' ]
      };

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveSnapshot({ aggregateId, revision: 5, state });

      const snapshot = await sparbuch.getSnapshot(aggregateId);

      assert.that(snapshot).is.equalTo({
        revision: 5,
        state
      });
    });

    test('correctly handles null, undefined and empty arrays.', async () => {
      const aggregateId = uuid();

      const state = {
        initiator: null,
        destination: undefined,
        participants: []
      };

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveSnapshot({ aggregateId, revision: 5, state });

      const snapshot = await sparbuch.getSnapshot(aggregateId);

      assert.that(snapshot.revision).is.equalTo(5);
      assert.that(snapshot.state).is.equalTo({
        initiator: null,
        participants: []
      });
    });

    test('returns the newest snapshot for the given aggregate.', async () => {
      const aggregateId = uuid();

      const stateOld = {
        initiator: 'Jane Doe',
        destination: 'Riva',
        participants: [ 'Jane Doe' ]
      };

      const stateNew = {
        initiator: 'Jane Doe',
        destination: 'Moulou',
        participants: [ 'Jane Doe', 'Jenny Doe' ]
      };

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveSnapshot({ aggregateId, revision: 5, state: stateOld });
      await sparbuch.saveSnapshot({ aggregateId, revision: 10, state: stateNew });

      const snapshot = await sparbuch.getSnapshot(aggregateId);

      assert.that(snapshot).is.equalTo({
        revision: 10,
        state: stateNew
      });
    });
  });

  suite('saveSnapshot', () => {
    test('is a function.', async () => {
      assert.that(sparbuch.saveSnapshot).is.ofType('function');
    });

    test('throws an error if aggregate id is missing.', async () => {
      await assert.that(async () => {
        await sparbuch.saveSnapshot({});
      }).is.throwingAsync('Aggregate id is missing.');
    });

    test('throws an error if revision is missing.', async () => {
      await assert.that(async () => {
        await sparbuch.saveSnapshot({ aggregateId: uuid() });
      }).is.throwingAsync('Revision is missing.');
    });

    test('throws an error if state is missing.', async () => {
      await assert.that(async () => {
        await sparbuch.saveSnapshot({ aggregateId: uuid(), revision: 10 });
      }).is.throwingAsync('State is missing.');
    });

    test('saves a snapshot.', async () => {
      const aggregateId = uuid();
      const state = {
        initiator: 'Jane Doe',
        destination: 'Riva',
        participants: [ 'Jane Doe' ]
      };

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveSnapshot({ aggregateId, revision: 10, state });

      const snapshot = await sparbuch.getSnapshot(aggregateId);

      assert.that(snapshot).is.equalTo({
        revision: 10,
        state
      });
    });

    test('correctly handles null, undefined and empty arrays.', async () => {
      const aggregateId = uuid();
      const state = {
        initiator: null,
        destination: undefined,
        participants: []
      };

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveSnapshot({ aggregateId, revision: 10, state });

      const snapshot = await sparbuch.getSnapshot(aggregateId);

      assert.that(snapshot).is.equalTo({
        revision: 10,
        state: {
          initiator: null,
          participants: []
        }
      });
    });

    test('does not throw an error if trying to save an already saved snapshot.', async () => {
      const aggregateId = uuid();
      const state = {
        initiator: 'Jane Doe',
        destination: 'Riva',
        participants: [ 'Jane Doe' ]
      };

      await sparbuch.initialize({ url, namespace });
      await sparbuch.saveSnapshot({ aggregateId, revision: 10, state });
      await sparbuch.saveSnapshot({ aggregateId, revision: 10, state });
    });
  });

  suite('getReplay', () => {
    test('is a function.', async () => {
      assert.that(sparbuch.getReplay).is.ofType('function');
    });

    test('throws an error if fromPosition is greater than toPosition.', async () => {
      await assert.that(async () => {
        await sparbuch.getReplay({ fromPosition: 23, toPosition: 7 });
      }).is.throwingAsync('From position is greater than to position.');
    });

    test('returns an empty stream.', async () => {
      await sparbuch.initialize({ url, namespace });

      const replayStream = await sparbuch.getReplay();
      const replayEvents = await toArray(replayStream);

      assert.that(replayEvents.length).is.equalTo(0);
    });

    suite('with existent data', () => {
      setup(async () => {
        const eventStarted = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: uuid() },
          name: 'started',
          data: { initiator: 'Jane Doe', destination: 'Riva' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        const eventJoinedFirst = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: eventStarted.aggregate.id },
          name: 'joined',
          data: { participant: 'Jane Doe' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        const eventJoinedSecond = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: eventStarted.aggregate.id },
          name: 'joined',
          data: { participant: 'Jennifer Doe' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        eventStarted.metadata.revision = 1;
        eventJoinedFirst.metadata.revision = 2;
        eventJoinedSecond.metadata.revision = 3;

        await sparbuch.initialize({ url, namespace });
        await sparbuch.saveEvents({ events: [ eventStarted, eventJoinedFirst, eventJoinedSecond ]});
      });

      test('returns all events if no options are given.', async () => {
        const replayStream = await sparbuch.getReplay();
        const replayEvents = await toArray(replayStream);

        assert.that(replayEvents.length).is.equalTo(3);
        assert.that(replayEvents[0].name).is.equalTo('started');
        assert.that(replayEvents[0].metadata.position).is.equalTo(1);
        assert.that(replayEvents[1].name).is.equalTo('joined');
        assert.that(replayEvents[1].metadata.position).is.equalTo(2);
        assert.that(replayEvents[2].name).is.equalTo('joined');
        assert.that(replayEvents[2].metadata.position).is.equalTo(3);
      });

      test('returns all events from the given position.', async () => {
        const replayStream = await sparbuch.getReplay({ fromPosition: 2 });
        const replayEvents = await toArray(replayStream);

        assert.that(replayEvents.length).is.equalTo(2);
        assert.that(replayEvents[0].name).is.equalTo('joined');
        assert.that(replayEvents[0].metadata.position).is.equalTo(2);
        assert.that(replayEvents[1].name).is.equalTo('joined');
        assert.that(replayEvents[1].metadata.position).is.equalTo(3);
      });

      test('returns all events to the given position.', async () => {
        const replayStream = await sparbuch.getReplay({ toPosition: 2 });
        const replayEvents = await toArray(replayStream);

        assert.that(replayEvents.length).is.equalTo(2);
        assert.that(replayEvents[0].name).is.equalTo('started');
        assert.that(replayEvents[0].metadata.position).is.equalTo(1);
        assert.that(replayEvents[1].name).is.equalTo('joined');
        assert.that(replayEvents[1].metadata.position).is.equalTo(2);
      });

      test('returns all events between the given positions.', async () => {
        const replayStream = await sparbuch.getReplay({ fromPosition: 2, toPosition: 2 });
        const replayEvents = await toArray(replayStream);

        assert.that(replayEvents.length).is.equalTo(1);
        assert.that(replayEvents[0].name).is.equalTo('joined');
        assert.that(replayEvents[0].metadata.position).is.equalTo(2);
      });
    });
  });

  suite('destroy', () => {
    test('is a function.', async () => {
      assert.that(sparbuch.destroy).is.ofType('function');
    });
  });
};
/* eslint-enable mocha/max-top-level-suites */

module.exports = getTestsFor;
