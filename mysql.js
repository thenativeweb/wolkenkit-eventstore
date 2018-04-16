'use strict';

const Eventstore = require('./dist/mysql/Eventstore');

const eventstore = new Eventstore();

module.exports = eventstore;
