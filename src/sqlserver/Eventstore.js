'use strict';

const { EventEmitter } = require('events'),
      { PassThrough } = require('stream');

const cloneDeep = require('lodash/cloneDeep'),
      { Event } = require('commands-events'),
      flatten = require('lodash/flatten'),
      genericPool = require('generic-pool'),
      limitAlphanumeric = require('limit-alphanumeric'),
      { parse } = require('pg-connection-string'),
      { Request } = require('tedious');

const createPool = require('./createPool'),
      omitByDeep = require('../omitByDeep');

class Eventstore extends EventEmitter {
  async getDatabase () {
    const database = await this.pool.acquire();

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

    this.pool = createPool({ host, port, user, password, database });

    this.pool.on('error', () => {
      this.emit('disconnect');
    });

    this.pool.on('disconnect', () => {
      this.emit('disconnect');
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
  }

  async getUnpublishedEventStream () {
  }

  async saveEvents ({ events }) {

  }

  async markEventsAsPublished ({ aggregateId, fromRevision, toRevision }) {

  }

  async getSnapshot (aggregateId) {
  }

  async saveSnapshot ({ aggregateId, revision, state }) {

  }

  async getReplay (options) {

  }

  async destroy () {
    if (this.pool) {
      await this.pool.clear();
    }
  }
}

module.exports = Eventstore;
