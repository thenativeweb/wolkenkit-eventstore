'use strict';

const { EventEmitter } = require('events'),
      { PassThrough } = require('stream');

const cloneDeep = require('lodash/cloneDeep'),
      { Event } = require('commands-events'),
      flatten = require('lodash/flatten'),
      limitAlphanumeric = require('limit-alphanumeric'),
      { parse } = require('pg-connection-string'),
      mysql = require('mysql2/promise');

const omitByDeep = require('../omitByDeep');

class Eventstore extends EventEmitter {
  async getDatabase () {
    const database = await this.pool.getConnection();

    return database;
  }

  async initialize ({ url, namespace }) {
    if (!url) {
      throw new Error('Url is missing.');
    }
    if (!namespace) {
      throw new Error('Namespace is missing.');
    }

    this.namespace = `store_${limitAlphanumeric(namespace)}`;

    const { host, port, user, password, database } = parse(url);

    this.pool = mysql.createPool({
      host,
      port,
      user,
      password,
      database,
      multipleStatements: true
    });

    this.pool.on('connection', connection => {
      connection.on('error', () => {
        this.emit('disconnect');
      });
      connection.on('end', () => {
        this.emit('disconnect');
      });
    });

    const connection = await this.getDatabase();

    const createUuidToBinFunction = `
      CREATE FUNCTION UuidToBin(_uuid BINARY(36))
        RETURNS BINARY(16)
        RETURN UNHEX(CONCAT(
          SUBSTR(_uuid, 15, 4),
          SUBSTR(_uuid, 10, 4),
          SUBSTR(_uuid, 1, 8),
          SUBSTR(_uuid, 20, 4),
          SUBSTR(_uuid, 25)
        ));
    `;

    try {
      await connection.query(createUuidToBinFunction);
    } catch (ex) {
      // If the function already exists, we can ignore this error; otherwise
      // rethrow it. Generally speaking, this should be done using a SQL clause
      // such as 'IF NOT EXISTS', but MySQL does not support this yet. Also,
      // there is a ready-made function UUID_TO_BIN, but this is only available
      // from MySQL 8.0 upwards.
      if (!ex.message.includes('FUNCTION UuidToBin already exists')) {
        throw ex;
      }
    }

    const createUuidFromBinFunction = `
      CREATE FUNCTION UuidFromBin(_bin BINARY(16))
        RETURNS BINARY(36)
        RETURN LCASE(CONCAT_WS('-',
          HEX(SUBSTR(_bin,  5, 4)),
          HEX(SUBSTR(_bin,  3, 2)),
          HEX(SUBSTR(_bin,  1, 2)),
          HEX(SUBSTR(_bin,  9, 2)),
          HEX(SUBSTR(_bin, 11))
        ));
    `;

    try {
      await connection.query(createUuidFromBinFunction);
    } catch (ex) {
      // If the function already exists, we can ignore this error; otherwise
      // rethrow it. Generally speaking, this should be done using a SQL clause
      // such as 'IF NOT EXISTS', but MySQL does not support this yet. Also,
      // there is a ready-made function BIN_TO_UUID, but this is only available
      // from MySQL 8.0 upwards.
      if (!ex.message.includes('FUNCTION UuidFromBin already exists')) {
        throw ex;
      }
    }

    const query = `
      CREATE TABLE IF NOT EXISTS ${this.namespace}_events (
        position SERIAL,
        aggregateId BINARY(16) NOT NULL,
        revision INT NOT NULL,
        event JSON NOT NULL,
        hasBeenPublished BOOLEAN NOT NULL,

        PRIMARY KEY(position),
        UNIQUE (aggregateId, revision)
      ) ENGINE = InnoDB;

      CREATE TABLE IF NOT EXISTS ${this.namespace}_snapshots (
        aggregateId BINARY(16) NOT NULL,
        revision INT NOT NULL,
        state JSON NOT NULL,

        PRIMARY KEY(aggregateId, revision)
      ) ENGINE = InnoDB;
    `;

    await connection.query(query);

    await connection.release();
  }

  async getLastEvent (aggregateId) {
    if (!aggregateId) {
      throw new Error('Aggregate id is missing.');
    }

    const connection = await this.getDatabase();

    try {
      const [ rows ] = await connection.execute(`
        SELECT event, position
          FROM ${this.namespace}_events
            WHERE aggregateId = UuidToBin(?)
          ORDER BY revision DESC
          LIMIT 1
        `, [ aggregateId ]);

      if (rows.length === 0) {
        return;
      }

      const event = Event.wrap(rows[0].event);

      event.metadata.position = Number(rows[0].position);

      return event;
    } finally {
      await connection.release();
    }
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

    const connection = await this.getDatabase();

    const passThrough = new PassThrough({ objectMode: true });
    const eventStream = connection.connection.execute(`
      SELECT event, position, hasBeenPublished
        FROM ${this.namespace}_events
        WHERE aggregateId = UuidToBin(?)
          AND revision >= ?
          AND revision <= ?
        ORDER BY revision`,
    [ aggregateId, fromRevision, toRevision ]);

    let onEnd,
        onError,
        onResult;

    const unsubscribe = function () {
      connection.release();
      eventStream.removeListener('end', onEnd);
      eventStream.removeListener('error', onError);
      eventStream.removeListener('result', onResult);
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

    onResult = function (row) {
      const event = Event.wrap(row.event);

      event.metadata.position = Number(row.position);
      event.metadata.published = Boolean(row.hasBeenPublished);

      passThrough.write(event);
    };

    eventStream.on('end', onEnd);
    eventStream.on('error', onError);
    eventStream.on('result', onResult);

    return passThrough;
  }

  async getUnpublishedEventStream () {
    const connection = await this.getDatabase();

    const passThrough = new PassThrough({ objectMode: true });
    const eventStream = connection.connection.execute(`
      SELECT event, position, hasBeenPublished
        FROM ${this.namespace}_events
        WHERE hasBeenPublished = false
        ORDER BY position
    `);

    let onEnd,
        onError,
        onResult;

    const unsubscribe = function () {
      connection.release();
      eventStream.removeListener('end', onEnd);
      eventStream.removeListener('error', onError);
      eventStream.removeListener('result', onResult);
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

    onResult = function (row) {
      const event = Event.wrap(row.event);

      event.metadata.position = Number(row.position);
      event.metadata.published = Boolean(row.hasBeenPublished);
      passThrough.write(event);
    };

    eventStream.on('end', onEnd);
    eventStream.on('error', onError);
    eventStream.on('result', onResult);

    return passThrough;
  }

  async saveEvents ({ events }) {
    if (!events) {
      throw new Error('Events are missing.');
    }

    events = cloneDeep(flatten([ events ]));

    const connection = await this.getDatabase();

    try {
      for (let i = 0; i < events.length; i++) {
        const event = events[i];

        await connection.execute(`
          INSERT INTO ${this.namespace}_events
            (aggregateId, revision, event, hasBeenPublished)
            VALUES (UuidToBin(?), ?, ?, ?);
        `, [ event.aggregate.id, event.metadata.revision, JSON.stringify(event), event.metadata.published ]);

        const [ rows ] = await connection.execute('SELECT LAST_INSERT_ID() AS position;');

        events[i].metadata.position = Number(rows[0].position);
      }

      return events;
    } catch (ex) {
      if (ex.code === 'ER_DUP_ENTRY' && ex.sqlMessage.endsWith('for key \'aggregateId\'')) {
        throw new Error('Aggregate id and revision already exist.');
      }

      throw ex;
    } finally {
      await connection.release();
    }
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

    const connection = await this.getDatabase();

    try {
      await connection.execute(`
        UPDATE ${this.namespace}_events
          SET hasBeenPublished = true
          WHERE aggregateId = UuidToBin(?)
            AND revision >= ?
            AND revision <= ?
      `, [ aggregateId, fromRevision, toRevision ]);
    } finally {
      await connection.release();
    }
  }

  async getSnapshot (aggregateId) {
    if (!aggregateId) {
      throw new Error('Aggregate id is missing.');
    }

    const connection = await this.getDatabase();

    try {
      const [ rows ] = await connection.execute(`
        SELECT state, revision
          FROM ${this.namespace}_snapshots
          WHERE aggregateId = UuidToBin(?)
          ORDER BY revision DESC
          LIMIT 1
      `, [ aggregateId ]);

      if (rows.length === 0) {
        return;
      }

      return {
        revision: rows[0].revision,
        state: rows[0].state
      };
    } finally {
      await connection.release();
    }
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

    const connection = await this.getDatabase();

    try {
      await connection.execute(`
        INSERT IGNORE INTO ${this.namespace}_snapshots
          (aggregateId, revision, state)
          VALUES (UuidToBin(?), ?, ?);
      `, [ aggregateId, revision, JSON.stringify(state) ]);
    } finally {
      await connection.release();
    }
  }

  async getReplay (options) {
    options = options || {};

    const fromPosition = options.fromPosition || 1;
    const toPosition = options.toPosition || 2 ** 31 - 1;

    if (fromPosition > toPosition) {
      throw new Error('From position is greater than to position.');
    }

    const connection = await this.getDatabase();

    const passThrough = new PassThrough({ objectMode: true });
    const eventStream = connection.connection.execute(`
      SELECT event, position
        FROM ${this.namespace}_events
        WHERE position >= ?
          AND position <= ?
        ORDER BY position
      `, [ fromPosition, toPosition ]);

    let onEnd,
        onError,
        onResult;

    const unsubscribe = function () {
      connection.release();
      eventStream.removeListener('end', onEnd);
      eventStream.removeListener('error', onError);
      eventStream.removeListener('result', onResult);
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

    onResult = function (row) {
      const event = Event.wrap(row.event);

      event.metadata.position = Number(row.position);
      passThrough.write(event);
    };

    eventStream.on('end', onEnd);
    eventStream.on('error', onError);
    eventStream.on('result', onResult);

    return passThrough;
  }

  async destroy () {
    if (this.pool) {
      await this.pool.end();
    }
  }
}

module.exports = Eventstore;
