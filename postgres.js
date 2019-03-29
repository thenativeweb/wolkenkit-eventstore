'use strict';

const Eventstore = require('./lib/postgres/Eventstore');

const eventstore = new Eventstore();

module.exports = eventstore;
