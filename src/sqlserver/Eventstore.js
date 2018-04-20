'use strict';

const { EventEmitter } = require('events'),
      { PassThrough } = require('stream');

const cloneDeep = require('lodash/cloneDeep'),
      { Event } = require('commands-events'),
      flatten = require('lodash/flatten'),
      limitAlphanumeric = require('limit-alphanumeric'),
      { parse } = require('pg-connection-string'),
      { Request, TYPES } = require('tedious');

const createPool = require('./createPool'),
      omitByDeep = require('../omitByDeep');

class Eventstore extends EventEmitter {
  async getDatabase () {
    const database = await this.pool.acquire().promise;

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

    this.pool = createPool({
      host,
      port,
      user,
      password,
      database,

      onError: () => {
        this.emit('error');
      },

      onDisconnect: () => {
        this.emit('disconnect');
      }
    });

    const connection = await this.getDatabase();

    const query = `
      IF NOT EXISTS (SELECT [name] FROM sys.tables WHERE [name] = '${this.namespace}_events')
        BEGIN
          CREATE TABLE [${this.namespace}_events] (
            [position] BIGINT IDENTITY(1,1),
            [aggregateId] UNIQUEIDENTIFIER NOT NULL,
            [revision] INT NOT NULL,
            [event] NVARCHAR(4000) NOT NULL,
            [hasBeenPublished] BIT NOT NULL,

            CONSTRAINT [${this.namespace}_events_pk] PRIMARY KEY([position]),
            CONSTRAINT [${this.namespace}_aggregateId_revision] UNIQUE ([aggregateId], [revision])
          );
        END

      IF NOT EXISTS (SELECT [name] FROM sys.tables WHERE [name] = '${this.namespace}_snapshots')
        BEGIN
          CREATE TABLE [${this.namespace}_snapshots] (
            [aggregateId] UNIQUEIDENTIFIER NOT NULL,
            [revision] INT NOT NULL,
            [state] NVARCHAR(4000) NOT NULL,

            CONSTRAINT [${this.namespace}_snapshots_pk] PRIMARY KEY([aggregateId], [revision])
          );
        END
    `;

    await new Promise((resolve, reject) => {
      const request = new Request(query, err => {
        if (err) {
          return reject(err);
        }

        resolve();
      });

      connection.execSql(request);
    });

    await this.pool.release(connection);
  }

  async getLastEvent (aggregateId) {

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

    const database = await this.getDatabase();

    const passThrough = new PassThrough({ objectMode: true });

    let onError,
        onRow,
        request;

    const unsubscribe = () => {
      // TODO: Can this be done with await?
      this.pool.release(database);
      request.removeListener('row', onRow);
      request.removeListener('error', onError);
    };

    onError = err => {
      unsubscribe();
      passThrough.emit('error', err);
      passThrough.end();
    };

    onRow = cols => {
      const event = Event.wrap(JSON.parse(cols[0].value));

      event.metadata.position = Number(cols[1].value);
      event.metadata.published = cols[2].value;

      passThrough.write(event);
    };

    request = new Request(`
      SELECT [event], [position], [hasBeenPublished]
        FROM [${this.namespace}_events]
        WHERE [aggregateId] = @aggregateId
          AND [revision] >= @fromRevision
          AND [revision] <= @toRevision
        ORDER BY [revision]`, err => {
      unsubscribe();

      if (err) {
        passThrough.emit('error', err);
      }

      passThrough.end();
    });

    request.addParameter('aggregateId', TYPES.UniqueIdentifier, aggregateId);
    request.addParameter('fromRevision', TYPES.Int, fromRevision);
    request.addParameter('toRevision', TYPES.Int, toRevision);

    request.on('row', onRow);
    request.on('error', onError);

    database.execSql(request);

    return passThrough;
  }

  async getUnpublishedEventStream () {
  }

  async saveEvents ({ events }) {
    if (!events) {
      throw new Error('Events are missing.');
    }

    events = cloneDeep(flatten([ events ]));

    const database = await this.getDatabase();

    // TODO: wrap this in transaction

    try {
      await new Promise((resolve, reject) => {
        const bulkLoad = database.newBulkLoad(`${this.namespace}_events`, { tableLock: true }, (err, rowCount) => {
          if (err) {
            return reject(err);
          }

          resolve();
        });

        bulkLoad.addColumn('aggregateId', TYPES.UniqueIdentifier, { nullable: false });
        bulkLoad.addColumn('revision', TYPES.Int, { nullable: false });
        bulkLoad.addColumn('event', TYPES.NVarChar, { length: 4000, nullable: false });
        bulkLoad.addColumn('hasBeenPublished', TYPES.Bit, { nullable: false });

        for (let i = 0; i < events.length; i++) {
          const event = events[i];

          bulkLoad.addRow({
            aggregateId: event.aggregate.id,
            revision: event.metadata.revision,
            event: JSON.stringify(event),
            hasBeenPublished: event.metadata.published
          });
        }

        database.execBulkLoad(bulkLoad);
      });

      await new Promise((resolve, reject) => {
        const request = new Request(`SELECT TOP(${events.length}) position, aggregateId FROM ${this.namespace}_events ORDER BY position ASC`, err => {
          if (err) {
            return reject(err);
          }

          resolve(events);
        });

        request.on('row', cols => {
          const position = Number(cols[0].value);
          const aggregateId = cols[1].value.toLowerCase();

          const foundEvent = events.find(event => event.aggregate.id === aggregateId);

          foundEvent.metadata.position = position;
        });

        database.execSql(request);
      });
    } catch (ex) {
      if (ex.code === 'EREQUEST' && ex.number === 2627 && ex.message.includes('_aggregateId_revision')) {
        throw new Error('Aggregate id and revision already exist.');
      }

      throw ex;
    } finally {
      await this.pool.release(database);
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

    const database = await this.getDatabase();

    try {
      await new Promise((resolve, reject) => {
        const request = new Request(`
          UPDATE [${this.namespace}_events]
            SET [hasBeenPublished] = 1
            WHERE [aggregateId] = @aggregateId
              AND [revision] >= @fromRevision
              AND [revision] <= @toRevision
          `, err => {
          if (err) {
            return reject(err);
          }

          resolve();
        });

        request.addParameter('aggregateId', TYPES.UniqueIdentifier, aggregateId);
        request.addParameter('fromRevision', TYPES.Int, fromRevision);
        request.addParameter('toRevision', TYPES.Int, toRevision);

        database.execSql(request);
      });
    } finally {
      await this.pool.release(database);
    }
  }

  async getSnapshot (aggregateId) {
  }

  async saveSnapshot ({ aggregateId, revision, state }) {

  }

  async getReplay (options) {

  }

  async destroy () {
    if (this.pool) {
      await this.pool.destroy();
    }
  }
}

module.exports = Eventstore;
