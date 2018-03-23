'use strict';

const { EventEmitter } = require('events'),
      { PassThrough } = require('stream');

const cloneDeep = require('lodash/cloneDeep'),
      { Event } = require('commands-events'),
      flatten = require('lodash/flatten'),
      limitAlphanumeric = require('limit-alphanumeric');

const omitByDeep = require('../omitByDeep');

class Sparbuch extends EventEmitter {
  async initialize ({ url, namespace }) {
    if (!url) {
      throw new Error('Url is missing.');
    }
    if (!namespace) {
      throw new Error('Namespace is missing.');
    }

    this.namespace = `store_${limitAlphanumeric(namespace)}`;

    this.database = {
      [`${this.namespace}_events`]: [],
      [`${this.namespace}_snapshots`]: []
    };
  }

  getStoredEvents () {
    return this.database[`${this.namespace}_events`];
  }

  getStoredSnapshots () {
    return this.database[`${this.namespace}_snapshots`];
  }

  storeEventAtDatabase (event) {
    this.database[`${this.namespace}_events`].push(event);
  }

  storeSnapshotAtDatabase (snapshot) {
    this.database[`${this.namespace}_snapshots`].push(snapshot);
  }

  async getLastEvent (aggregateId) {
    if (!aggregateId) {
      throw new Error('Aggregate id is missing.');
    }

    const eventsInDatabase = this.getStoredEvents().
      filter(event => event.aggregate.id === aggregateId);

    if (eventsInDatabase.length === 0) {
      return;
    }

    const lastEvent = eventsInDatabase[eventsInDatabase.length - 1];

    return Event.wrap(lastEvent);
  }

  async getEventStream (aggregateId, options) {
    if (!aggregateId) {
      throw new Error('Aggregate id is missing.');
    }

    options = options || {};

    const fromRevision = options.fromRevision || 1;
    const toRevision = options.toRevision || 2 ** 31 - 1;

    if (fromRevision > toRevision) {
      throw new Error('From revision is greater than to revision.');
    }

    const passThrough = new PassThrough({ objectMode: true });

    const filteredEvents = this.getStoredEvents().
      filter(event => event.aggregate.id === aggregateId &&
                      event.metadata.revision >= fromRevision &&
                      event.metadata.revision <= toRevision);

    filteredEvents.forEach(event => {
      passThrough.write(Event.wrap(event));
    });
    passThrough.end();

    return passThrough;
  }

  async getUnpublishedEventStream () {
    const filteredEvents = this.getStoredEvents().
      filter(event => event.metadata.published === false);

    const passThrough = new PassThrough({ objectMode: true });

    filteredEvents.forEach(event => {
      passThrough.write(Event.wrap(event));
    });
    passThrough.end();

    return passThrough;
  }

  async saveEvents ({ events }) {
    if (!events) {
      throw new Error('Events are missing.');
    }

    events = cloneDeep(flatten([ events ]));

    const eventsInDatabase = this.getStoredEvents();

    events.forEach(event => {
      if (
        eventsInDatabase.find(
          eventInDatabase =>
            event.aggregate.id === eventInDatabase.aggregate.id &&
            event.metadata.revision === eventInDatabase.metadata.revision
        )
      ) {
        throw new Error('Aggregate id and revision already exist.');
      }

      const newPosition = eventsInDatabase.length + 1;

      event.data = omitByDeep(event.data, value => value === undefined);
      event.metadata.position = newPosition;

      this.storeEventAtDatabase(event);
    });

    return events;
  }

  async markEventsAsPublished ({ aggregateId, fromRevision, toRevision }) {
    if (!aggregateId) {
      throw new Error('Aggregate id is missing.');
    }
    if (!fromRevision) {
      throw new Error('From revision is missing.');
    }
    if (!toRevision) {
      throw new Error('To revision is missing.');
    }

    if (fromRevision > toRevision) {
      throw new Error('From revision is greater than to revision.');
    }

    // just for tests
    this.database.events = [];
  }

  async getSnapshot (aggregateId) {
    if (!aggregateId) {
      throw new Error('Aggregate id is missing.');
    }

    const matchingSnapshotsForAggregateId = this.getStoredSnapshots().
      filter(snapshot => snapshot.aggregateId === aggregateId);

    const newestSnapshotRevision = Math.max(
      ...matchingSnapshotsForAggregateId.map(snapshot => snapshot.revision)
    );

    const matchingSnapshot = matchingSnapshotsForAggregateId.
      find(snapshot => snapshot.revision === newestSnapshotRevision);

    if (!matchingSnapshot) {
      return;
    }

    return {
      revision: matchingSnapshot.revision,
      state: matchingSnapshot.state
    };
  }

  async saveSnapshot ({ aggregateId, revision, state }) {
    if (!aggregateId) {
      throw new Error('Aggregate id is missing.');
    }
    if (!revision) {
      throw new Error('Revision is missing.');
    }
    if (!state) {
      throw new Error('State is missing.');
    }

    state = omitByDeep(state, value => value === undefined);

    const snapshot = {
      aggregateId,
      revision,
      state
    };

    this.storeSnapshotAtDatabase(snapshot);
  }

  async getReplay (options) {
    options = options || {};

    const fromPosition = options.fromPosition || 1;
    const toPosition = options.toPosition || 2 ** 31 - 1;

    if (fromPosition > toPosition) {
      throw new Error('From position is greater than to position.');
    }

    const passThrough = new PassThrough({ objectMode: true });

    const filteredEvents = this.getStoredEvents().
      filter(event => event.metadata.position >= fromPosition &&
                      event.metadata.position <= toPosition);

    filteredEvents.forEach(event => {
      passThrough.write(event);
    });
    passThrough.end();

    return passThrough;
  }

  /* eslint-disable*/
  async destroy () {
  }
  /* eslint-enable*/
}

module.exports = Sparbuch;
