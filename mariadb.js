'use strict';

const Eventstore = require('./lib/mariadb/Eventstore');

const eventstore = new Eventstore();

module.exports = eventstore;
