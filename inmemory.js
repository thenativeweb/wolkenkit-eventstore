'use strict';

const Eventstore = require('./lib/inmemory/Eventstore');

const eventstore = new Eventstore();

module.exports = eventstore;
