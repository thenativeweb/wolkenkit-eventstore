'use strict';

const { EventEmitter } = require('events'),
      { PassThrough } = require('stream');

const cloneDeep = require('lodash/cloneDeep'),
      flatten = require('lodash/flatten'),
      limitAlphanumeric = require('limit-alphanumeric');

const omitByDeep = require('../omitByDeep');

class Sparbuch extends EventEmitter {
  async getDatabase () {
    return this.database;
  }

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

  async getLastEvent (aggregateId) {
    if (!aggregateId) {
      throw new Error('Aggregate id is missing.');
    }

    const eventsInDatabase = this.database[`${this.namespace}_events`].
      filter(event => event.event.aggregate.id === aggregateId);

    if (eventsInDatabase.length === 0) {
      return;
    }

    return eventsInDatabase[eventsInDatabase.length - 1].event;
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

    const eventsInDatabase = this.database[`${this.namespace}_events`];

    const filteredEvents = eventsInDatabase.
      filter(event => event.aggregateId === aggregateId &&
                      event.event.metadata.revision >= fromRevision &&
                      event.event.metadata.revision <= toRevision);

    filteredEvents.forEach(event => {
      passThrough.write(event.event);
    });
    passThrough.end();

    return passThrough;
  }

  async getUnpublishedEventStream () {
    const eventsInDatabase = this.database[`${this.namespace}_events`];

    const filteredEvents = eventsInDatabase.
      filter(event => event.event.metadata.published === false);

    const passThrough = new PassThrough({ objectMode: true });

    filteredEvents.forEach(event => {
      passThrough.write(event.event);
    });
    passThrough.end();

    return passThrough;
  }

  async saveEvents ({ events }) {
    if (!events) {
      throw new Error('Events are missing.');
    }

    events = cloneDeep(flatten([ events ]));

    const database = await this.getDatabase();

    const eventsInDatabase = database[`${this.namespace}_events`];

    events.forEach(event => {
      if (eventsInDatabase.find(evt => event.aggregate.id === evt.aggregateId && event.metadata.revision === evt.revision)) {
        throw new Error('Aggregate id and revision already exist.');
      }

      const newEvent = {
        aggregateId: event.aggregate.id,
        revision: event.metadata.revision,
        event,
        hasBeenPublished: event.metadata.published
      };

      const newPosition = eventsInDatabase.length + 1;

      newEvent.event.metadata.position = newPosition;
      newEvent.position = newPosition;

      // Remove keys with value undefined
      newEvent.event.data = Object.keys(event.data).reduce((acc, key) => {
        if (typeof event.data[key] !== 'undefined') {
          acc[key] = event.data[key];
        }

        return acc;
      }, {});

      database[`${this.namespace}_events`].push(newEvent);
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

    const database = await this.getDatabase();

    // just for tests
    database.events = [];
  }

  async getSnapshot (aggregateId) {
    if (!aggregateId) {
      throw new Error('Aggregate id is missing.');
    }

    const snapshotsInDatabase = this.database[`${this.namespace}_snapshots`];

    const matchingSnapshotsForAggregateId = snapshotsInDatabase.
      filter(snapshot => snapshot.aggregateId === aggregateId);
    const newestSnapshotRevision = Math.max(...matchingSnapshotsForAggregateId.map(snapshot => snapshot.revision));

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

    const newSnapshot = {
      aggregateId,
      revision,
      state
    };

    this.database[`${this.namespace}_snapshots`].push(newSnapshot);
  }

  async getReplay (options) {
    options = options || {};

    const fromPosition = options.fromPosition || 1;
    const toPosition = options.toPosition || 2 ** 31 - 1;

    if (fromPosition > toPosition) {
      throw new Error('From position is greater than to position.');
    }

    const passThrough = new PassThrough({ objectMode: true });

    const eventsInDatabase = this.database[`${this.namespace}_events`];

    const filteredEvents = eventsInDatabase.
      filter(event => event.position >= fromPosition &&
                      event.position <= toPosition);

    filteredEvents.forEach(event => {
      passThrough.write(event.event);
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
