'use strict';

const Eventstore = require('./dist/postgres/Eventstore');

const eventstore = new Eventstore();

module.exports = eventstore;
