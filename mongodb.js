'use strict';

const Eventstore = require('./lib/mongodb/Eventstore');

const eventstore = new Eventstore();

module.exports = eventstore;
