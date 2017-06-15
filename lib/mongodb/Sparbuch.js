'use strict';

const EventEmitter = require('events').EventEmitter,
      stream = require('stream'),
      util = require('util');

const _ = require('lodash'),
      async = require('async'),
      Event = require('commands-events').Event,
      limitAlphanumeric = require('limit-alphanumeric'),
      MongoClient = require('mongodb').MongoClient;

const omitByDeep = require('../omitByDeep');

const PassThrough = stream.PassThrough;

const Sparbuch = function () {
  this.db = undefined;
  this.collections = {};
};

util.inherits(Sparbuch, EventEmitter);

Sparbuch.prototype.initialize = function (options, callback) {
  if (!options) {
    throw new Error('Options are missing.');
  }
  if (!options.url) {
    throw new Error('Url is missing.');
  }
  if (!options.namespace) {
    throw new Error('Namespace is missing.');
  }
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.namespace = `store_${limitAlphanumeric(options.namespace)}`;

  /* eslint-disable id-length */
  MongoClient.connect(options.url, { w: 1 }, (errConnect, db) => {
    /* eslint-enable id-length */
    if (errConnect) {
      return callback(errConnect);
    }

    this.db = db;

    db.on('close', () => {
      this.emit('disconnect');
    });

    this.collections.events = db.collection(`${options.namespace}_events`);
    this.collections.snapshots = db.collection(`${options.namespace}_snapshots`);
    this.collections.counters = db.collection(`${options.namespace}_counters`);

    async.series([
      done => this.collections.events.ensureIndex({ 'aggregate.id': 1 }, { name: `${this.namespace}_aggregateId` }, done),
      done => this.collections.events.ensureIndex({ 'aggregate.id': 1, 'metadata.revision': 1 }, { unique: true, name: `${this.namespace}_aggregateId_revision` }, done),
      done => this.collections.events.ensureIndex({ 'metadata.position': 1 }, { unique: true, name: `${this.namespace}_position` }, done),
      done => this.collections.snapshots.ensureIndex({ 'aggregate.id': 1 }, { unique: true }, done),
      done => {
        this.collections.counters.insertOne({ _id: 'events', seq: 0 }, err => {
          if (err) {
            if (err.code === 11000 && err.message.includes('_counters index: _id_ dup key')) {
              return done(null);
            }

            return done(err);
          }
          done(null);
        });
      }
    ], err => {
      if (err) {
        return callback(err);
      }
      callback(null);
    });
  });
};

Sparbuch.prototype.getNextSequence = function (name, callback) {
  this.collections.counters.findOneAndUpdate({ _id: name }, {
    $inc: { seq: 1 }
  }, { returnOriginal: false }, (err, counter) => {
    if (err) {
      return callback(err);
    }
    callback(null, counter.value.seq);
  });
};

Sparbuch.prototype.getLastEvent = function (aggregateId, callback) {
  if (!aggregateId) {
    throw new Error('Aggregate id is missing.');
  }
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.collections.events.find({
    'aggregate.id': aggregateId
  }, {
    fields: { _id: 0 },
    sort: { 'metadata.revision': -1 },
    limit: 1
  }).toArray((err, events) => {
    if (err) {
      return callback(err);
    }
    if (events.length === 0) {
      return callback(null, undefined);
    }

    callback(null, Event.wrap(events[0]));
  });
};

Sparbuch.prototype.getEventStream = function (aggregateId, options, callback) {
  if (!callback) {
    callback = options;
    options = undefined;
  }

  if (!aggregateId) {
    throw new Error('Aggregate id is missing.');
  }
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  options = options || {};
  options.fromRevision = options.fromRevision || 1;
  options.toRevision = options.toRevision || (Math.pow(2, 31) - 1);

  if (options.fromRevision > options.toRevision) {
    throw new Error('From revision is greater than to revision.');
  }

  const passThrough = new PassThrough({ objectMode: true });
  const eventStream = this.collections.events.find({
    $and: [
      { 'aggregate.id': aggregateId },
      { 'metadata.revision': { $gte: options.fromRevision }},
      { 'metadata.revision': { $lte: options.toRevision }}
    ]
  }, {
    fields: { _id: 0 },
    sort: 'metadata.revision'
  }).stream();

  let onData,
      onEnd,
      onError;

  const unsubscribe = function () {
    eventStream.removeListener('data', onData);
    eventStream.removeListener('end', onEnd);
    eventStream.removeListener('error', onError);
  };

  onData = function (data) {
    passThrough.write(Event.wrap(data));
  };

  onEnd = function () {
    unsubscribe();
    passThrough.end();

    // In the PostgreSQL sparbuch, we call eventStream.end() here. In MongoDB,
    // this function apparently is not implemented. This note is just for
    // informational purposes to ensure that you are aware that the two
    // implementations differ here.
  };

  onError = function (err) {
    unsubscribe();
    passThrough.emit('error', err);
    passThrough.end();

    // In the PostgreSQL sparbuch, we call eventStream.end() here. In MongoDB,
    // this function apparently is not implemented. This note is just for
    // informational purposes to ensure that you are aware that the two
    // implementations differ here.
  };

  eventStream.on('data', onData);
  eventStream.on('end', onEnd);
  eventStream.on('error', onError);

  callback(null, passThrough);
};

Sparbuch.prototype.getUnpublishedEventStream = function (callback) {
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  const passThrough = new PassThrough({ objectMode: true });
  const eventStream = this.collections.events.find({
    'metadata.published': false
  }, {
    fields: { _id: 0 },
    sort: 'metadata.position'
  }).stream();

  let onData,
      onEnd,
      onError;

  const unsubscribe = function () {
    eventStream.removeListener('data', onData);
    eventStream.removeListener('end', onEnd);
    eventStream.removeListener('error', onError);
  };

  onData = function (data) {
    passThrough.write(Event.wrap(data));
  };

  onEnd = function () {
    unsubscribe();
    passThrough.end();

    // In the PostgreSQL sparbuch, we call eventStream.end() here. In MongoDB,
    // this function apparently is not implemented. This note is just for
    // informational purposes to ensure that you are aware that the two
    // implementations differ here.
  };

  onError = function (err) {
    unsubscribe();
    passThrough.emit('error', err);
    passThrough.end();

    // In the PostgreSQL sparbuch, we call eventStream.end() here. In MongoDB,
    // this function apparently is not implemented. This note is just for
    // informational purposes to ensure that you are aware that the two
    // implementations differ here.
  };

  eventStream.on('data', onData);
  eventStream.on('end', onEnd);
  eventStream.on('error', onError);

  callback(null, passThrough);
};

Sparbuch.prototype.saveEvents = function (options, callback) {
  if (!options) {
    throw new Error('Options are missing.');
  }
  if (!options.events) {
    throw new Error('Events are missing.');
  }
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  const events = _.cloneDeep(_.flatten([ options.events ]));

  async.eachSeries(events, (event, done) => {
    this.getNextSequence('events', (err, seq) => {
      if (err) {
        return done(err);
      }

      event.data = omitByDeep(event.data, value => value === undefined);
      event.metadata.position = seq;

      // Use cloned events here to hinder MongoDB from adding an _id property to
      // the original event objects.
      this.collections.events.insertOne(_.cloneDeep(event), done);
    });
  }, err => {
    if (err) {
      if (err.code === 11000 && err.message.indexOf('_aggregateId_revision') !== -1) {
        return callback(new Error('Aggregate id and revision already exist.'));
      }

      return callback(err);
    }
    callback(null, events);
  });
};

Sparbuch.prototype.markEventsAsPublished = function (options, callback) {
  if (!options) {
    throw new Error('Options are missing.');
  }
  if (!options.aggregateId) {
    throw new Error('Aggregate id is missing.');
  }
  if (!options.fromRevision) {
    throw new Error('From revision is missing.');
  }
  if (!options.toRevision) {
    throw new Error('To revision is missing.');
  }
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  if (options.fromRevision > options.toRevision) {
    throw new Error('From revision is greater than to revision.');
  }

  this.collections.events.update({
    'aggregate.id': options.aggregateId,
    'metadata.revision': {
      $gte: options.fromRevision,
      $lte: options.toRevision
    }
  }, {
    $set: {
      'metadata.published': true
    }
  }, {
    multi: true
  }, callback);
};

Sparbuch.prototype.getSnapshot = function (aggregateId, callback) {
  if (!aggregateId) {
    throw new Error('Aggregate id is missing.');
  }
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.collections.snapshots.findOne({ aggregateId }, {
    fields: { _id: 0, revision: 1, state: 1 }
  }, (err, snapshot) => {
    if (err) {
      return callback(err);
    }
    if (!snapshot) {
      return callback(null, undefined);
    }

    callback(null, snapshot);
  });
};

Sparbuch.prototype.saveSnapshot = function (options, callback) {
  if (!options) {
    throw new Error('Options are missing.');
  }
  if (!options.aggregateId) {
    throw new Error('Aggregate id is missing.');
  }
  if (!options.revision) {
    throw new Error('Revision is missing.');
  }
  if (!options.state) {
    throw new Error('State is missing.');
  }
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  options.state = omitByDeep(options.state, value => value === undefined);

  this.collections.snapshots.update({
    aggregateId: options.aggregateId
  }, {
    aggregateId: options.aggregateId,
    state: options.state,
    revision: options.revision
  }, {
    upsert: true
  }, callback);
};

Sparbuch.prototype.getReplay = function (options, callback) {
  if (!callback) {
    if (typeof options !== 'function') {
      throw new Error('Callback is missing.');
    }

    callback = options;
  }

  options = options || {};
  options.fromPosition = options.fromPosition || 1;
  options.toPosition = options.toPosition || (Math.pow(2, 31) - 1);

  if (options.fromPosition > options.toPosition) {
    throw new Error('From position is greater than to position.');
  }

  const passThrough = new PassThrough({ objectMode: true });
  const replayStream = this.collections.events.find({
    $and: [
      { 'metadata.position': { $gte: options.fromPosition }},
      { 'metadata.position': { $lte: options.toPosition }}
    ]
  }, {
    fields: { _id: 0 },
    sort: 'metadata.position'
  }).stream();

  let onData,
      onEnd,
      onError;

  const unsubscribe = function () {
    replayStream.removeListener('data', onData);
    replayStream.removeListener('end', onEnd);
    replayStream.removeListener('error', onError);
  };

  onData = function (data) {
    passThrough.write(Event.wrap(data));
  };

  onEnd = function () {
    unsubscribe();
    passThrough.end();

    // In the PostgreSQL sparbuch, we call replayStream.end() here. In MongoDB,
    // this function apparently is not implemented. This note is just for
    // informational purposes to ensure that you are aware that the two
    // implementations differ here.
  };

  onError = function (err) {
    unsubscribe();
    passThrough.emit('error', err);
    passThrough.end();

    // In the PostgreSQL sparbuch, we call replayStream.end() here. In MongoDB,
    // this function apparently is not implemented. This note is just for
    // informational purposes to ensure that you are aware that the two
    // implementations differ here.
  };

  replayStream.on('data', onData);
  replayStream.on('end', onEnd);
  replayStream.on('error', onError);

  callback(null, passThrough);
};

Sparbuch.prototype.destroy = function (callback) {
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  if (!this.db) {
    return process.nextTick(() => callback(null));
  }
  this.db.close(true, callback);
};

module.exports = Sparbuch;
