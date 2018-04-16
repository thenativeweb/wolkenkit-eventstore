'use strict';

const Eventstore = require('./dist/mongodb/Eventstore');

const eventstore = new Eventstore();

module.exports = eventstore;
