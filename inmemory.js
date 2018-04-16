'use strict';

const Eventstore = require('./dist/inmemory/Eventstore');

const eventstore = new Eventstore();

module.exports = eventstore;
