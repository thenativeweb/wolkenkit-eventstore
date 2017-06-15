'use strict';

const events = require('events');

const assert = require('assertthat'),
      Event = require('commands-events').Event,
      highland = require('highland'),
      uuid = require('uuidv4');

const EventEmitter = events.EventEmitter;

/* eslint-disable mocha/max-top-level-suites */
const getTestsFor = function (Sparbuch, options) {
  let namespace,
      sparbuch;

  setup(() => {
    sparbuch = new Sparbuch();
    namespace = uuid();
  });

  teardown(done => {
    sparbuch.destroy(done);
  });

  test('is a function.', done => {
    assert.that(Sparbuch).is.ofType('function');
    done();
  });

  test('is an event emitter.', done => {
    assert.that(sparbuch).is.instanceOf(EventEmitter);
    done();
  });

  test('emits a disconnect event when the connection to the database becomes lost.', function (done) {
    this.timeout(15 * 1000);

    sparbuch.initialize({ url: options.url, namespace }, err => {
      assert.that(err).is.null();

      sparbuch.once('disconnect', () => {
        options.startContainer(done);
      });

      options.stopContainer();
    });
  });

  suite('initialize', () => {
    test('is a function.', done => {
      assert.that(sparbuch.initialize).is.ofType('function');
      done();
    });

    test('throws an error if options are missing.', done => {
      assert.that(() => {
        sparbuch.initialize();
      }).is.throwing('Options are missing.');
      done();
    });

    test('throws an error if url is missing.', done => {
      assert.that(() => {
        sparbuch.initialize({});
      }).is.throwing('Url is missing.');
      done();
    });

    test('throws an error if namespace is missing.', done => {
      assert.that(() => {
        sparbuch.initialize({ url: options.url });
      }).is.throwing('Namespace is missing.');
      done();
    });

    test('throws an error if callback is missing.', done => {
      assert.that(() => {
        sparbuch.initialize({ url: options.url, namespace });
      }).is.throwing('Callback is missing.');
      done();
    });

    test('returns an error if the database is not reachable.', done => {
      sparbuch.initialize({ url: options.nonExistentUrl, namespace }, err => {
        assert.that(err).is.not.null();
        done();
      });
    });

    test('does not return an error if the database is reachable.', done => {
      sparbuch.initialize({ url: options.url, namespace }, err => {
        assert.that(err).is.null();
        done();
      });
    });

    test('does not return an error if tables, indexes & co. do already exist.', done => {
      sparbuch.initialize({ url: options.url, namespace }, errFirst => {
        assert.that(errFirst).is.null();
        sparbuch.initialize({ url: options.url, namespace }, errSecond => {
          assert.that(errSecond).is.null();
          done();
        });
      });
    });

    test('returns an error if the aggregate id and revision of the new event are already in use.', done => {
      const event = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      event.metadata.revision = 1;

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveEvents({ events: event }, errSaveEvents1 => {
          assert.that(errSaveEvents1).is.null();

          sparbuch.saveEvents({ events: event }, errSaveEvents2 => {
            assert.that(errSaveEvents2).is.not.null();
            assert.that(errSaveEvents2.message).is.equalTo('Aggregate id and revision already exist.');
            done();
          });
        });
      });
    });

    test('does not call the callback multiple times if an error occurs within the callback.', done => {
      const listeners = process.listeners('uncaughtException');

      process.removeAllListeners('uncaughtException');

      process.once('uncaughtException', err => {
        assert.that(err.message).is.equalTo('foo');

        listeners.forEach(listener => {
          process.on('uncaughtException', listener);
        });

        done();
      });

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();
        throw new Error('foo');
      });
    });

    suite('event stream order', () => {
      test('assigns the position 1 to the first event.', done => {
        const event = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: uuid() },
          name: 'started',
          data: { initiator: 'Jane Doe', destination: 'Riva' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        event.metadata.revision = 1;

        sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
          assert.that(errInitialize).is.null();

          sparbuch.saveEvents({ events: event }, errSaveEvents => {
            assert.that(errSaveEvents).is.null();

            sparbuch.getEventStream(event.aggregate.id, (err, eventStream) => {
              assert.that(err).is.null();

              highland(eventStream).toArray(aggregateEvents => {
                assert.that(aggregateEvents.length).is.equalTo(1);
                assert.that(aggregateEvents[0].metadata.position).is.equalTo(1);
                done();
              });
            });
          });
        });
      });

      test('assigns increasing positions to subsequent events.', done => {
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

        sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
          assert.that(errInitialize).is.null();

          sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]}, errSaveEvents => {
            assert.that(errSaveEvents).is.null();

            sparbuch.getEventStream(eventStarted.aggregate.id, (err, eventStream) => {
              assert.that(err).is.null();

              highland(eventStream).toArray(aggregateEvents => {
                assert.that(aggregateEvents.length).is.equalTo(2);
                assert.that(aggregateEvents[0].metadata.position).is.equalTo(1);
                assert.that(aggregateEvents[1].metadata.position).is.equalTo(2);
                done();
              });
            });
          });
        });
      });

      test('assigns increasing positions even when saving the events individually.', done => {
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

        sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
          assert.that(errInitialize).is.null();

          sparbuch.saveEvents({ events: eventStarted }, errSaveEventStarted => {
            assert.that(errSaveEventStarted).is.null();

            sparbuch.saveEvents({ events: eventJoined }, errSaveEventJoined => {
              assert.that(errSaveEventJoined).is.null();

              sparbuch.getEventStream(eventStarted.aggregate.id, (err, eventStream) => {
                assert.that(err).is.null();

                highland(eventStream).toArray(aggregateEvents => {
                  assert.that(aggregateEvents.length).is.equalTo(2);
                  assert.that(aggregateEvents[0].metadata.position).is.equalTo(1);
                  assert.that(aggregateEvents[1].metadata.position).is.equalTo(2);
                  done();
                });
              });
            });
          });
        });
      });

      test('ensures that positions are unique across aggregates.', done => {
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

        sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
          assert.that(errInitialize).is.null();

          sparbuch.saveEvents({ events: eventStarted1 }, errSaveEventStarted1 => {
            assert.that(errSaveEventStarted1).is.null();

            sparbuch.saveEvents({ events: eventStarted2 }, errSaveEventStarted2 => {
              assert.that(errSaveEventStarted2).is.null();

              sparbuch.getEventStream(eventStarted1.aggregate.id, (errEventStream1, eventStream1) => {
                assert.that(errEventStream1).is.null();

                highland(eventStream1).toArray(aggregateEvents1 => {
                  assert.that(aggregateEvents1.length).is.equalTo(1);
                  assert.that(aggregateEvents1[0].metadata.position).is.equalTo(1);

                  sparbuch.getEventStream(eventStarted2.aggregate.id, (errEventStream2, eventStream2) => {
                    assert.that(errEventStream2).is.null();

                    highland(eventStream2).toArray(aggregateEvents2 => {
                      assert.that(aggregateEvents2.length).is.equalTo(1);
                      assert.that(aggregateEvents2[0].metadata.position).is.equalTo(2);
                      done();
                    });
                  });
                });
              });
            });
          });
        });
      });

      test('returns the saved events enriched by their positions.', done => {
        const event = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: uuid() },
          name: 'started',
          data: { initiator: 'Jane Doe', destination: 'Riva' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        event.metadata.revision = 1;

        sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
          assert.that(errInitialize).is.null();

          sparbuch.saveEvents({ events: event }, (errSaveEvents, savedEvents) => {
            assert.that(errSaveEvents).is.null();
            assert.that(savedEvents.length).is.equalTo(1);
            assert.that(savedEvents[0].metadata.position).is.equalTo(1);
            done();
          });
        });
      });

      test('does not change the events that were given as arguments.', done => {
        const event = new Event({
          context: { name: 'planning' },
          aggregate: { name: 'peerGroup', id: uuid() },
          name: 'started',
          data: { initiator: 'Jane Doe', destination: 'Riva' },
          metadata: { correlationId: uuid(), causationId: uuid() }
        });

        event.metadata.revision = 1;

        sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
          assert.that(errInitialize).is.null();

          sparbuch.saveEvents({ events: event }, errSaveEvents => {
            assert.that(errSaveEvents).is.null();
            assert.that(event.metadata.position).is.undefined();
            done();
          });
        });
      });
    });
  });

  suite('getLastEvent', () => {
    test('is a function.', done => {
      assert.that(sparbuch.getLastEvent).is.ofType('function');
      done();
    });

    test('throws an error if aggregate id is missing.', done => {
      assert.that(() => {
        sparbuch.getLastEvent();
      }).is.throwing('Aggregate id is missing.');
      done();
    });

    test('throws an error if callback is missing.', done => {
      assert.that(() => {
        sparbuch.getLastEvent(uuid());
      }).is.throwing('Callback is missing.');
      done();
    });

    test('returns undefined for a aggregate without events.', done => {
      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.getLastEvent(uuid(), (err, event) => {
          assert.that(err).is.null();
          assert.that(event).is.undefined();
          done();
        });
      });
    });

    test('returns the last event for the given aggregate.', done => {
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

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]}, errSaveEvents => {
          assert.that(errSaveEvents).is.null();

          sparbuch.getLastEvent(aggregateId, (err, event) => {
            assert.that(err).is.null();
            assert.that(event.name).is.equalTo('joined');
            assert.that(event.metadata.revision).is.equalTo(2);
            done();
          });
        });
      });
    });

    test('correctly handles null, undefined and empty arrays.', done => {
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

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveEvents({ events: [ eventJoined ]}, errSaveEvents => {
          assert.that(errSaveEvents).is.null();

          sparbuch.getLastEvent(aggregateId, (err, event) => {
            assert.that(err).is.null();
            assert.that(event.data.initiator).is.null();
            assert.that(event.data.participants).is.equalTo([]);
            done();
          });
        });
      });
    });

    test('does not call the callback multiple times if an error occurs within the callback.', done => {
      const aggregateId = uuid();

      const eventStarted = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: aggregateId },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      eventStarted.metadata.revision = 1;

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveEvents({ events: [ eventStarted ]}, errSaveEvents => {
          assert.that(errSaveEvents).is.null();

          const listeners = process.listeners('uncaughtException');

          process.removeAllListeners('uncaughtException');

          process.once('uncaughtException', err => {
            assert.that(err.message).is.equalTo('foo');

            listeners.forEach(listener => {
              process.on('uncaughtException', listener);
            });

            done();
          });

          sparbuch.getLastEvent(aggregateId, err => {
            assert.that(err).is.null();
            throw new Error('foo');
          });
        });
      });
    });

    test('does not call the callback multiple times if an error occurs within the callback even if no event was found.', done => {
      const aggregateId = uuid();

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        const listeners = process.listeners('uncaughtException');

        process.removeAllListeners('uncaughtException');

        process.once('uncaughtException', err => {
          assert.that(err.message).is.equalTo('foo');

          listeners.forEach(listener => {
            process.on('uncaughtException', listener);
          });

          done();
        });

        sparbuch.getLastEvent(aggregateId, err => {
          assert.that(err).is.null();
          throw new Error('foo');
        });
      });
    });
  });

  suite('getEventStream', () => {
    test('is a function.', done => {
      assert.that(sparbuch.getEventStream).is.ofType('function');
      done();
    });

    test('throws an error if aggregate id is missing.', done => {
      assert.that(() => {
        sparbuch.getEventStream();
      }).is.throwing('Aggregate id is missing.');
      done();
    });

    test('throws an error if callback is missing.', done => {
      assert.that(() => {
        sparbuch.getEventStream(uuid());
      }).is.throwing('Callback is missing.');
      done();
    });

    test('throws an error if from revision is greater than to revision.', done => {
      assert.that(() => {
        sparbuch.getEventStream(uuid(), { fromRevision: 42, toRevision: 23 }, () => {
          // Never called...
        });
      }).is.throwing('From revision is greater than to revision.');
      done();
    });

    test('returns an empty stream for a non-existent aggregate.', done => {
      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.getEventStream(uuid(), (err, eventStream) => {
          assert.that(err).is.null();

          highland(eventStream).toArray(aggregateEvents => {
            assert.that(aggregateEvents.length).is.equalTo(0);
            done();
          });
        });
      });
    });

    test('returns a stream of events for the given aggregate.', done => {
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

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]}, errSaveEvents => {
          assert.that(errSaveEvents).is.null();

          sparbuch.getEventStream(eventStarted.aggregate.id, (err, eventStream) => {
            assert.that(err).is.null();

            highland(eventStream).toArray(aggregateEvents => {
              assert.that(aggregateEvents.length).is.equalTo(2);
              assert.that(aggregateEvents[0].name).is.equalTo('started');
              assert.that(aggregateEvents[1].name).is.equalTo('joined');
              done();
            });
          });
        });
      });
    });

    test('returns a stream from revision.', done => {
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

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]}, errSaveEvents => {
          assert.that(errSaveEvents).is.null();

          sparbuch.getEventStream(eventStarted.aggregate.id, { fromRevision: 2 }, (err, eventStream) => {
            assert.that(err).is.null();

            highland(eventStream).toArray(aggregateEvents => {
              assert.that(aggregateEvents.length).is.equalTo(1);
              assert.that(aggregateEvents[0].name).is.equalTo('joined');
              done();
            });
          });
        });
      });
    });

    test('returns a stream to revision.', done => {
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

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]}, errSaveEvents => {
          assert.that(errSaveEvents).is.null();

          sparbuch.getEventStream(eventStarted.aggregate.id, { toRevision: 1 }, (err, eventStream) => {
            assert.that(err).is.null();

            highland(eventStream).toArray(aggregateEvents => {
              assert.that(aggregateEvents.length).is.equalTo(1);
              assert.that(aggregateEvents[0].name).is.equalTo('started');
              done();
            });
          });
        });
      });
    });
  });

  suite('getUnpublishedEventStream', () => {
    test('is a function.', done => {
      assert.that(sparbuch.getUnpublishedEventStream).is.ofType('function');
      done();
    });

    test('throws an error if callback is missing.', done => {
      assert.that(() => {
        sparbuch.getUnpublishedEventStream();
      }).is.throwing('Callback is missing.');
      done();
    });

    test('returns an empty stream if there are no unpublished events.', done => {
      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.getUnpublishedEventStream((err, eventStream) => {
          assert.that(err).is.null();

          highland(eventStream).toArray(unpublishedEvents => {
            assert.that(unpublishedEvents.length).is.equalTo(0);
            done();
          });
        });
      });
    });

    test('returns a stream of unpublished events.', done => {
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

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]}, errSaveEvents => {
          assert.that(errSaveEvents).is.null();

          sparbuch.getUnpublishedEventStream((err, eventStream) => {
            assert.that(err).is.null();

            highland(eventStream).toArray(unpublishedEvents => {
              assert.that(unpublishedEvents.length).is.equalTo(1);
              assert.that(unpublishedEvents[0].name).is.equalTo('joined');
              done();
            });
          });
        });
      });
    });
  });

  suite('saveEvents', () => {
    test('is a function.', done => {
      assert.that(sparbuch.saveEvents).is.ofType('function');
      done();
    });

    test('throws an error if options are missing.', done => {
      assert.that(() => {
        sparbuch.saveEvents();
      }).is.throwing('Options are missing.');
      done();
    });

    test('throws an error if events are missing.', done => {
      assert.that(() => {
        sparbuch.saveEvents({});
      }).is.throwing('Events are missing.');
      done();
    });

    test('throws an error if callback is missing.', done => {
      assert.that(() => {
        sparbuch.saveEvents({ events: {}});
      }).is.throwing('Callback is missing.');
      done();
    });

    test('saves a single event.', done => {
      const event = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      event.metadata.revision = 1;

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveEvents({ events: event }, errSaveEvents => {
          assert.that(errSaveEvents).is.null();

          sparbuch.getEventStream(event.aggregate.id, (err, eventStream) => {
            assert.that(err).is.null();

            highland(eventStream).toArray(aggregateEvents => {
              assert.that(aggregateEvents.length).is.equalTo(1);
              assert.that(aggregateEvents[0].name).is.equalTo('started');
              done();
            });
          });
        });
      });
    });

    test('saves multiple events.', done => {
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

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveEvents({ events: [ eventStarted, eventJoined ]}, errSaveEvents => {
          assert.that(errSaveEvents).is.null();

          sparbuch.getEventStream(eventStarted.aggregate.id, (err, eventStream) => {
            assert.that(err).is.null();

            highland(eventStream).toArray(aggregateEvents => {
              assert.that(aggregateEvents.length).is.equalTo(2);
              assert.that(aggregateEvents[0].name).is.equalTo('started');
              assert.that(aggregateEvents[1].name).is.equalTo('joined');
              done();
            });
          });
        });
      });
    });

    test('correctly handles undefined and null.', done => {
      const event = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: null, destination: undefined },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      event.metadata.revision = 1;

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveEvents({ events: event }, errSaveEvents => {
          assert.that(errSaveEvents).is.null();

          sparbuch.getEventStream(event.aggregate.id, (err, eventStream) => {
            assert.that(err).is.null();

            highland(eventStream).toArray(aggregateEvents => {
              assert.that(aggregateEvents.length).is.equalTo(1);
              assert.that(aggregateEvents[0].data).is.equalTo({ initiator: null });
              done();
            });
          });
        });
      });
    });

    test('does not call the callback multiple times if an error occurs within the callback.', done => {
      const event = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      event.metadata.revision = 1;

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        const listeners = process.listeners('uncaughtException');

        process.removeAllListeners('uncaughtException');

        process.once('uncaughtException', err => {
          assert.that(err.message).is.equalTo('foo');

          listeners.forEach(listener => {
            process.on('uncaughtException', listener);
          });

          done();
        });

        sparbuch.saveEvents({ events: event }, errSaveEvents => {
          assert.that(errSaveEvents).is.null();
          throw new Error('foo');
        });
      });
    });
  });

  suite('markEventsAsPublished', () => {
    test('is a function.', done => {
      assert.that(sparbuch.markEventsAsPublished).is.ofType('function');
      done();
    });

    test('throws an error if options are missing.', done => {
      assert.that(() => {
        sparbuch.markEventsAsPublished();
      }).is.throwing('Options are missing.');
      done();
    });

    test('throws an error if aggregate id is missing.', done => {
      assert.that(() => {
        sparbuch.markEventsAsPublished({});
      }).is.throwing('Aggregate id is missing.');
      done();
    });

    test('throws an error if from revision is missing.', done => {
      assert.that(() => {
        sparbuch.markEventsAsPublished({ aggregateId: uuid() });
      }).is.throwing('From revision is missing.');
      done();
    });

    test('throws an error if to revision is missing.', done => {
      assert.that(() => {
        sparbuch.markEventsAsPublished({ aggregateId: uuid(), fromRevision: 5 });
      }).is.throwing('To revision is missing.');
      done();
    });

    test('throws an error if callback is missing.', done => {
      assert.that(() => {
        sparbuch.markEventsAsPublished({ aggregateId: uuid(), fromRevision: 5, toRevision: 10 });
      }).is.throwing('Callback is missing.');
      done();
    });

    test('marks the specified events as published.', done => {
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

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveEvents({ events: [ eventStarted, eventJoinedFirst, eventJoinedSecond ]}, errSaveEvents => {
          assert.that(errSaveEvents).is.null();

          sparbuch.markEventsAsPublished({
            aggregateId: eventStarted.aggregate.id,
            fromRevision: 1,
            toRevision: 2
          }, err => {
            assert.that(err).is.null();
            done();
          });
        });
      });
    });

    test('does not call the callback multiple times if an error occurs within the callback.', done => {
      const event = new Event({
        context: { name: 'planning' },
        aggregate: { name: 'peerGroup', id: uuid() },
        name: 'started',
        data: { initiator: 'Jane Doe', destination: 'Riva' },
        metadata: { correlationId: uuid(), causationId: uuid() }
      });

      event.metadata.revision = 1;

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveEvents({ events: event }, errSaveEvents => {
          assert.that(errSaveEvents).is.null();

          const listeners = process.listeners('uncaughtException');

          process.removeAllListeners('uncaughtException');

          process.once('uncaughtException', err => {
            assert.that(err.message).is.equalTo('foo');

            listeners.forEach(listener => {
              process.on('uncaughtException', listener);
            });

            done();
          });

          sparbuch.markEventsAsPublished({
            aggregateId: event.aggregate.id,
            fromRevision: 1,
            toRevision: 1
          }, err => {
            assert.that(err).is.null();
            throw new Error('foo');
          });
        });
      });
    });
  });

  suite('getSnapshot', () => {
    test('is a function.', done => {
      assert.that(sparbuch.getSnapshot).is.ofType('function');
      done();
    });

    test('throws an error if aggregate id is missing.', done => {
      assert.that(() => {
        sparbuch.getSnapshot();
      }).is.throwing('Aggregate id is missing.');
      done();
    });

    test('throws an error if callback is missing.', done => {
      assert.that(() => {
        sparbuch.getSnapshot(uuid());
      }).is.throwing('Callback is missing.');
      done();
    });

    test('returns undefined for a aggregate without a snapshot.', done => {
      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.getSnapshot(uuid(), (err, snapshot) => {
          assert.that(err).is.null();
          assert.that(snapshot).is.undefined();
          done();
        });
      });
    });

    test('returns a snapshot for the given aggregate.', done => {
      const aggregateId = uuid();

      const state = {
        initiator: 'Jane Doe',
        destination: 'Riva',
        participants: [ 'Jane Doe' ]
      };

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveSnapshot({ aggregateId, revision: 5, state }, errSaveSnapshot => {
          assert.that(errSaveSnapshot).is.null();

          sparbuch.getSnapshot(aggregateId, (err, snapshot) => {
            assert.that(err).is.null();
            assert.that(snapshot).is.equalTo({
              revision: 5,
              state
            });
            done();
          });
        });
      });
    });

    test('correctly handles null, undefined and empty arrays.', done => {
      const aggregateId = uuid();

      const state = {
        initiator: null,
        destination: undefined,
        participants: []
      };

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveSnapshot({ aggregateId, revision: 5, state }, errSaveSnapshot => {
          assert.that(errSaveSnapshot).is.null();

          sparbuch.getSnapshot(aggregateId, (err, snapshot) => {
            assert.that(err).is.null();
            assert.that(snapshot.revision).is.equalTo(5);
            assert.that(snapshot.state).is.equalTo({
              initiator: null,
              participants: []
            });
            done();
          });
        });
      });
    });

    test('returns the newest snapshot for the given aggregate.', done => {
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

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveSnapshot({ aggregateId, revision: 5, state: stateOld }, errSaveSnapshotOld => {
          assert.that(errSaveSnapshotOld).is.null();

          sparbuch.saveSnapshot({ aggregateId, revision: 10, state: stateNew }, errSaveSnapshotNew => {
            assert.that(errSaveSnapshotNew).is.null();

            sparbuch.getSnapshot(aggregateId, (err, snapshot) => {
              assert.that(err).is.null();
              assert.that(snapshot).is.equalTo({
                revision: 10,
                state: stateNew
              });
              done();
            });
          });
        });
      });
    });

    test('does not call the callback multiple times if an error occurs within the callback.', done => {
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

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveSnapshot({ aggregateId, revision: 5, state: stateOld }, errSaveSnapshotOld => {
          assert.that(errSaveSnapshotOld).is.null();

          sparbuch.saveSnapshot({ aggregateId, revision: 10, state: stateNew }, errSaveSnapshotNew => {
            assert.that(errSaveSnapshotNew).is.null();

            const listeners = process.listeners('uncaughtException');

            process.removeAllListeners('uncaughtException');

            process.once('uncaughtException', err => {
              assert.that(err.message).is.equalTo('foo');

              listeners.forEach(listener => {
                process.on('uncaughtException', listener);
              });

              done();
            });

            sparbuch.getSnapshot(aggregateId, err => {
              assert.that(err).is.null();
              throw new Error('foo');
            });
          });
        });
      });
    });

    test('does not call the callback multiple times if an error occurs within the callback even if no snapshot was found.', done => {
      const aggregateId = uuid();

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        const listeners = process.listeners('uncaughtException');

        process.removeAllListeners('uncaughtException');

        process.once('uncaughtException', err => {
          assert.that(err.message).is.equalTo('foo');

          listeners.forEach(listener => {
            process.on('uncaughtException', listener);
          });

          done();
        });

        sparbuch.getSnapshot(aggregateId, err => {
          assert.that(err).is.null();
          throw new Error('foo');
        });
      });
    });
  });

  suite('saveSnapshot', () => {
    test('is a function.', done => {
      assert.that(sparbuch.saveSnapshot).is.ofType('function');
      done();
    });

    test('throws an error if options are missing.', done => {
      assert.that(() => {
        sparbuch.saveSnapshot();
      }).is.throwing('Options are missing.');
      done();
    });

    test('throws an error if aggregate id is missing.', done => {
      assert.that(() => {
        sparbuch.saveSnapshot({});
      }).is.throwing('Aggregate id is missing.');
      done();
    });

    test('throws an error if revision is missing.', done => {
      assert.that(() => {
        sparbuch.saveSnapshot({ aggregateId: uuid() });
      }).is.throwing('Revision is missing.');
      done();
    });

    test('throws an error if state is missing.', done => {
      assert.that(() => {
        sparbuch.saveSnapshot({ aggregateId: uuid(), revision: 10 });
      }).is.throwing('State is missing.');
      done();
    });

    test('throws an error if callback is missing.', done => {
      assert.that(() => {
        sparbuch.saveSnapshot({ aggregateId: uuid(), revision: 10, state: {}});
      }).is.throwing('Callback is missing.');
      done();
    });

    test('saves a snapshot.', done => {
      const aggregateId = uuid();
      const state = {
        initiator: 'Jane Doe',
        destination: 'Riva',
        participants: [ 'Jane Doe' ]
      };

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveSnapshot({ aggregateId, revision: 10, state }, errSaveSnapshot => {
          assert.that(errSaveSnapshot).is.null();

          sparbuch.getSnapshot(aggregateId, (err, snapshot) => {
            assert.that(err).is.null();
            assert.that(snapshot).is.equalTo({
              revision: 10,
              state
            });
            done();
          });
        });
      });
    });

    test('correctly handles null, undefined and empty arrays.', done => {
      const aggregateId = uuid();
      const state = {
        initiator: null,
        destination: undefined,
        participants: []
      };

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveSnapshot({ aggregateId, revision: 10, state }, errSaveSnapshot => {
          assert.that(errSaveSnapshot).is.null();

          sparbuch.getSnapshot(aggregateId, (err, snapshot) => {
            assert.that(err).is.null();
            assert.that(snapshot).is.equalTo({
              revision: 10,
              state: {
                initiator: null,
                participants: []
              }
            });
            done();
          });
        });
      });
    });

    test('does not return an error if trying to save an already saved snapshot.', done => {
      const aggregateId = uuid();
      const state = {
        initiator: 'Jane Doe',
        destination: 'Riva',
        participants: [ 'Jane Doe' ]
      };

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.saveSnapshot({ aggregateId, revision: 10, state }, errSaveSnapshot => {
          assert.that(errSaveSnapshot).is.null();

          sparbuch.saveSnapshot({ aggregateId, revision: 10, state }, errSaveSnapshotAgain => {
            assert.that(errSaveSnapshotAgain).is.null();
            done();
          });
        });
      });
    });

    test('does not call the callback multiple times if an error occurs within the callback.', done => {
      const aggregateId = uuid();
      const state = {
        initiator: 'Jane Doe',
        destination: 'Riva',
        participants: [ 'Jane Doe' ]
      };

      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        const listeners = process.listeners('uncaughtException');

        process.removeAllListeners('uncaughtException');

        process.once('uncaughtException', err => {
          assert.that(err.message).is.equalTo('foo');

          listeners.forEach(listener => {
            process.on('uncaughtException', listener);
          });

          done();
        });

        sparbuch.saveSnapshot({ aggregateId, revision: 10, state }, err => {
          assert.that(err).is.null();
          throw new Error('foo');
        });
      });
    });
  });

  suite('getReplay', () => {
    test('is a function.', done => {
      assert.that(sparbuch.getReplay).is.ofType('function');
      done();
    });

    test('throws an error if callback is missing.', done => {
      assert.that(() => {
        sparbuch.getReplay();
      }).is.throwing('Callback is missing.');
      done();
    });

    test('throws an error if callback is missing even if an options object is given.', done => {
      assert.that(() => {
        sparbuch.getReplay({ fromPosition: 7, toPosition: 23 });
      }).is.throwing('Callback is missing.');
      done();
    });

    test('throws an error if fromPosition is greater than toPosition.', done => {
      assert.that(() => {
        sparbuch.getReplay({ fromPosition: 23, toPosition: 7 }, () => {
          // Intentionally left blank.
        });
      }).is.throwing('From position is greater than to position.');
      done();
    });

    test('returns an empty stream.', done => {
      sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
        assert.that(errInitialize).is.null();

        sparbuch.getReplay((errGetReplay, replayStream) => {
          assert.that(errGetReplay).is.null();

          highland(replayStream).toArray(replayEvents => {
            assert.that(replayEvents.length).is.equalTo(0);
            done();
          });
        });
      });
    });

    suite('with existent data', () => {
      setup(done => {
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

        sparbuch.initialize({ url: options.url, namespace }, errInitialize => {
          assert.that(errInitialize).is.null();

          sparbuch.saveEvents({ events: [ eventStarted, eventJoinedFirst, eventJoinedSecond ]}, errSaveEvents => {
            assert.that(errSaveEvents).is.null();

            done();
          });
        });
      });

      test('returns all events if no options are given.', done => {
        sparbuch.getReplay((err, replayStream) => {
          assert.that(err).is.null();

          highland(replayStream).toArray(replayEvents => {
            assert.that(replayEvents.length).is.equalTo(3);
            assert.that(replayEvents[0].name).is.equalTo('started');
            assert.that(replayEvents[0].metadata.position).is.equalTo(1);
            assert.that(replayEvents[1].name).is.equalTo('joined');
            assert.that(replayEvents[1].metadata.position).is.equalTo(2);
            assert.that(replayEvents[2].name).is.equalTo('joined');
            assert.that(replayEvents[2].metadata.position).is.equalTo(3);
            done();
          });
        });
      });

      test('returns all events from the given position.', done => {
        sparbuch.getReplay({ fromPosition: 2 }, (err, replayStream) => {
          assert.that(err).is.null();

          highland(replayStream).toArray(replayEvents => {
            assert.that(replayEvents.length).is.equalTo(2);
            assert.that(replayEvents[0].name).is.equalTo('joined');
            assert.that(replayEvents[0].metadata.position).is.equalTo(2);
            assert.that(replayEvents[1].name).is.equalTo('joined');
            assert.that(replayEvents[1].metadata.position).is.equalTo(3);
            done();
          });
        });
      });

      test('returns all events to the given position.', done => {
        sparbuch.getReplay({ toPosition: 2 }, (err, replayStream) => {
          assert.that(err).is.null();

          highland(replayStream).toArray(replayEvents => {
            assert.that(replayEvents.length).is.equalTo(2);
            assert.that(replayEvents[0].name).is.equalTo('started');
            assert.that(replayEvents[0].metadata.position).is.equalTo(1);
            assert.that(replayEvents[1].name).is.equalTo('joined');
            assert.that(replayEvents[1].metadata.position).is.equalTo(2);
            done();
          });
        });
      });

      test('returns all events between the given positions.', done => {
        sparbuch.getReplay({ fromPosition: 2, toPosition: 2 }, (err, replayStream) => {
          assert.that(err).is.null();

          highland(replayStream).toArray(replayEvents => {
            assert.that(replayEvents.length).is.equalTo(1);
            assert.that(replayEvents[0].name).is.equalTo('joined');
            assert.that(replayEvents[0].metadata.position).is.equalTo(2);
            done();
          });
        });
      });
    });
  });

  suite('destroy', () => {
    test('is a function.', done => {
      assert.that(sparbuch.destroy).is.ofType('function');
      done();
    });

    test('throws an error if callback is missing.', done => {
      assert.that(() => {
        sparbuch.destroy();
      }).is.throwing('Callback is missing.');
      done();
    });
  });
};
/* eslint-enable mocha/max-top-level-suites */

module.exports = getTestsFor;
