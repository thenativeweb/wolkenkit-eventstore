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
      console.log('poool error');
      this.emit('disconnect');
    });

    this.pool.on('disconnect', () => {
      console.log('poool disconnect');
      this.emit('disconnect');
    });

    this.keepAliveConnection = await this.getDatabase();

    await this.pool.release(this.keepAliveConnection);
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
      await this.pool.drain();
      await this.pool.clear();
    }
  }
}

module.exports = Eventstore;
