'use strict';

const Eventstore = require('./lib/mysql/Eventstore');

const eventstore = new Eventstore();

module.exports = eventstore;
