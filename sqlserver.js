'use strict';

const Eventstore = require('./dist/sqlserver/Eventstore');

const eventstore = new Eventstore();

module.exports = eventstore;
