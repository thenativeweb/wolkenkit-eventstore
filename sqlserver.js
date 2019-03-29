'use strict';

const Eventstore = require('./lib/sqlserver/Eventstore');

const eventstore = new Eventstore();

module.exports = eventstore;
