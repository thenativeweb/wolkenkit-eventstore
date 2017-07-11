'use strict';

const EventEmitter = require('events').EventEmitter,
      stream = require('stream'),
      util = require('util');

const _ = require('lodash'),
      Event = require('commands-events').Event,
      limitAlphanumeric = require('limit-alphanumeric'),
      parse = require('pg-connection-string').parse,
      pg = require('pg'),
      QueryStream = require('pg-query-stream'),
      retry = require('retry');

const omitByDeep = require('../omitByDeep');

const PassThrough = stream.PassThrough;

const Sparbuch = function () {
  // Initialization is being done using the initialize function.
};

util.inherits(Sparbuch, EventEmitter);

Sparbuch.prototype.getDatabase = function (callback) {
  this.pool.connect((err, database, close) => {
    if (err) {
      return callback(err);
    }

    callback(null, database, close);
  });
};

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

  const disconnectWatcher = new pg.Client(parse(options.url));

  disconnectWatcher.on('error', () => {
    this.emit('disconnect');
  });
  disconnectWatcher.connect(() => {
    disconnectWatcher.on('end', () => {
      this.emit('disconnect');
    });
  });

  this.pool = new pg.Pool(parse(options.url));
  this.pool.on('error', () => {
    this.emit('disconnect');
  });

  this.getDatabase((errGetDatabase, database, close) => {
    if (errGetDatabase) {
      return callback(errGetDatabase);
    }

    const operation = retry.operation({
      retries: 3,
      minTimeout: 100,
      factor: 1
    });

    operation.attempt(() => {
      database.query(`
        CREATE TABLE IF NOT EXISTS "${this.namespace}_events" (
          "position" bigserial NOT NULL,
          "aggregateId" uuid NOT NULL,
          "revision" integer NOT NULL,
          "event" jsonb NOT NULL,
          "hasBeenPublished" boolean NOT NULL,

          CONSTRAINT "${this.namespace}_events_pk" PRIMARY KEY("position"),
          CONSTRAINT "${this.namespace}_aggregateId_revision" UNIQUE ("aggregateId", "revision")
        );
        CREATE TABLE IF NOT EXISTS "${this.namespace}_snapshots" (
          "aggregateId" uuid NOT NULL,
          "revision" integer NOT NULL,
          "state" jsonb NOT NULL,

          CONSTRAINT "${this.namespace}_snapshots_pk" PRIMARY KEY("aggregateId", "revision")
        );
      `, errQuery => {
        if (operation.retry(errQuery)) {
          return;
        }

        close();

        if (errQuery) {
          return callback(operation.mainError());
        }
        callback(null);
      });
    });
  });
};

Sparbuch.prototype.getLastEvent = function (aggregateId, callback) {
  if (!aggregateId) {
    throw new Error('Aggregate id is missing.');
  }
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.getDatabase((errGetDatabase, database, close) => {
    if (errGetDatabase) {
      return callback(errGetDatabase);
    }

    database.query({
      name: 'get last event',
      text: `
        SELECT "event", "position"
          FROM "${this.namespace}_events"
          WHERE "aggregateId" = $1
          ORDER BY "revision" DESC
          LIMIT 1
      `,
      values: [ aggregateId ]
    }, (errQuery, result) => {
      close();

      if (errQuery) {
        return callback(errQuery);
      }

      if (result.rows.length === 0) {
        return callback(null, undefined);
      }

      const event = Event.wrap(result.rows[0].event);

      event.metadata.position = Number(result.rows[0].position);

      callback(null, event);
    });
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

  this.getDatabase((errGetDatabase, database, close) => {
    if (errGetDatabase) {
      return callback(errGetDatabase);
    }

    const passThrough = new PassThrough({ objectMode: true });
    const eventStream = database.query(new QueryStream(`
      SELECT "event", "position"
        FROM "${this.namespace}_events"
        WHERE "aggregateId" = $1
          AND "revision" >= $2
          AND "revision" <= $3
        ORDER BY "revision"`,
      [ aggregateId, options.fromRevision, options.toRevision ]
    ));

    let onData,
        onEnd,
        onError;

    const unsubscribe = function () {
      close();
      eventStream.removeListener('data', onData);
      eventStream.removeListener('end', onEnd);
      eventStream.removeListener('error', onError);
    };

    onData = function (data) {
      const event = Event.wrap(data.event);

      event.metadata.position = Number(data.position);
      passThrough.write(event);
    };

    onEnd = function () {
      unsubscribe();
      passThrough.end();
    };

    onError = function (err) {
      unsubscribe();
      passThrough.emit('error', err);
      passThrough.end();
    };

    eventStream.on('data', onData);
    eventStream.on('end', onEnd);
    eventStream.on('error', onError);

    callback(null, passThrough);
  });
};

Sparbuch.prototype.getUnpublishedEventStream = function (callback) {
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.getDatabase((errGetDatabase, database, close) => {
    if (errGetDatabase) {
      return callback(errGetDatabase);
    }

    const passThrough = new PassThrough({ objectMode: true });
    const eventStream = database.query(new QueryStream(`
      SELECT "event", "position"
        FROM "${this.namespace}_events"
        WHERE "hasBeenPublished" = false
        ORDER BY "position"`
    ));

    let onData,
        onEnd,
        onError;

    const unsubscribe = function () {
      close();
      eventStream.removeListener('data', onData);
      eventStream.removeListener('end', onEnd);
      eventStream.removeListener('error', onError);
    };

    onData = function (data) {
      const event = Event.wrap(data.event);

      event.metadata.position = Number(data.position);
      passThrough.write(event);
    };

    onEnd = function () {
      unsubscribe();
      passThrough.end();
    };

    onError = function (err) {
      unsubscribe();
      passThrough.emit('error', err);
      passThrough.end();
    };

    eventStream.on('data', onData);
    eventStream.on('end', onEnd);
    eventStream.on('error', onError);

    callback(null, passThrough);
  });
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

  this.getDatabase((errGetDatabase, database, close) => {
    if (errGetDatabase) {
      return callback(errGetDatabase);
    }

    const placeholders = [],
          values = [];

    for (let i = 0; i < events.length; i++) {
      const base = 4 * i + 1,
            event = events[i];

      placeholders.push(`($${base}, $${base + 1}, $${base + 2}, $${base + 3})`);
      values.push(event.aggregate.id, event.metadata.revision, event, event.metadata.published);
    }

    const text = `
      INSERT INTO "${this.namespace}_events"
        ("aggregateId", "revision", "event", "hasBeenPublished")
      VALUES
        ${placeholders.join(',')} RETURNING position;
    `;

    database.query({ name: `save events ${events.length}`, text, values }, (errQuery, result) => {
      close();

      if (errQuery) {
        if (errQuery.code === '23505' && errQuery.detail.startsWith('Key ("aggregateId", revision)')) {
          return callback(new Error('Aggregate id and revision already exist.'));
        }

        return callback(errQuery);
      }

      for (let i = 0; i < result.rows.length; i++) {
        events[i].metadata.position = Number(result.rows[i].position);
      }

      callback(null, events);
    });
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

  this.getDatabase((errGetDatabase, database, close) => {
    if (errGetDatabase) {
      return callback(errGetDatabase);
    }

    database.query({
      name: 'mark events as published',
      text: `
        UPDATE "${this.namespace}_events"
          SET "hasBeenPublished" = true
          WHERE "aggregateId" = $1
            AND "revision" >= $2
            AND "revision" <= $3
      `,
      values: [ options.aggregateId, options.fromRevision, options.toRevision ]
    }, errQuery => {
      close();

      if (errQuery) {
        return callback(errQuery);
      }
      callback(null);
    });
  });
};

Sparbuch.prototype.getSnapshot = function (aggregateId, callback) {
  if (!aggregateId) {
    throw new Error('Aggregate id is missing.');
  }
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  this.getDatabase((errGetDatabase, database, close) => {
    if (errGetDatabase) {
      return callback(errGetDatabase);
    }

    database.query({
      name: 'get snapshot',
      text: `
        SELECT "state", "revision"
          FROM "${this.namespace}_snapshots"
          WHERE "aggregateId" = $1
          ORDER BY "revision" DESC
          LIMIT 1
      `,
      values: [ aggregateId ]
    }, (errQuery, result) => {
      close();

      if (errQuery) {
        return callback(errQuery);
      }

      if (result.rows.length === 0) {
        return callback(null, undefined);
      }

      callback(null, {
        revision: result.rows[0].revision,
        state: result.rows[0].state
      });
    });
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

  this.getDatabase((errGetDatabase, database, close) => {
    if (errGetDatabase) {
      return callback(errGetDatabase);
    }

    database.query({
      name: 'save snapshot',
      text: `
      INSERT INTO "${this.namespace}_snapshots" (
        "aggregateId", revision, state
      ) VALUES ($1, $2, $3)
      ON CONFLICT DO NOTHING;
      `,
      values: [ options.aggregateId, options.revision, options.state ]
    }, errQuery => {
      close();

      if (errQuery) {
        return callback(errQuery);
      }
      callback(null);
    });
  });
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

  this.getDatabase((errGetDatabase, database, close) => {
    if (errGetDatabase) {
      return callback(errGetDatabase);
    }

    const passThrough = new PassThrough({ objectMode: true });
    const eventStream = database.query(new QueryStream(`
      SELECT "event", "position"
        FROM "${this.namespace}_events"
        WHERE "position" >= $1
          AND "position" <= $2
        ORDER BY "position"`,
      [ options.fromPosition, options.toPosition ]
    ));

    let onData,
        onEnd,
        onError;

    const unsubscribe = function () {
      close();
      eventStream.removeListener('data', onData);
      eventStream.removeListener('end', onEnd);
      eventStream.removeListener('error', onError);
    };

    onData = function (data) {
      const event = Event.wrap(data.event);

      event.metadata.position = Number(data.position);
      passThrough.write(event);
    };

    onEnd = function () {
      unsubscribe();
      passThrough.end();
    };

    onError = function (err) {
      unsubscribe();
      passThrough.emit('error', err);
      passThrough.end();
    };

    eventStream.on('data', onData);
    eventStream.on('end', onEnd);
    eventStream.on('error', onError);

    callback(null, passThrough);
  });
};

Sparbuch.prototype.destroy = function (callback) {
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  if (!this.pool) {
    return process.nextTick(() => callback(null));
  }
  this.pool.end(callback);
};

module.exports = Sparbuch;
