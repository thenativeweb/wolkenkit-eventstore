'use strict';

const { EventEmitter } = require('events'),
      { PassThrough } = require('stream');

const cloneDeep = require('lodash/cloneDeep'),
      { Event } = require('commands-events'),
      flatten = require('lodash/flatten');

const omitByDeep = require('../omitByDeep');

class Eventstore extends EventEmitter {
  async initialize () {
    this.database = {
      events: [],
      snapshots: []
    };
  }

  getStoredEvents () {
    return this.database.events;
  }

  getStoredSnapshots () {
    return this.database.snapshots;
  }

  storeEventAtDatabase (event) {
    this.database.events.push(event);
  }

  storeSnapshotAtDatabase (snapshot) {
    this.database.snapshots.push(snapshot);
  }

  updateEventInDatabaseAtIndex (index, newEventData) {
    this.database.events[index] = newEventData;
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
    if (Array.isArray(events) && events.length === 0) {
      throw new Error('Events are missing.');
    }

    events = cloneDeep(flatten([ events ]));

    const eventsInDatabase = this.getStoredEvents();

    events.forEach(event => {
      if (!event.metadata) {
        throw new Error('Metadata are missing.');
      }
      if (event.metadata.revision === undefined) {
        throw new Error('Revision is missing.');
      }
      if (event.metadata.revision < 1) {
        throw new Error('Revision must not be less than 1.');
      }

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

    const eventsFromDatabase = this.getStoredEvents();

    const shouldEventBeMarkedAsPublished = event =>
      event.aggregate.id === aggregateId &&
      event.metadata.revision >= fromRevision &&
      event.metadata.revision <= toRevision;

    for (let i = 0; i < eventsFromDatabase.length; i++) {
      const event = eventsFromDatabase[i];

      if (shouldEventBeMarkedAsPublished(event)) {
        const eventToUpdate = cloneDeep(event);

        eventToUpdate.metadata.published = true;
        this.updateEventInDatabaseAtIndex(i, eventToUpdate);
      }
    }
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
      passThrough.write(Event.wrap(event));
    });
    passThrough.end();

    return passThrough;
  }

  /* eslint-disable*/
  async destroy () {
    this.database = {
      events: [],
      snapshots: []
    };
  }
  /* eslint-enable*/
}

module.exports = Eventstore;
