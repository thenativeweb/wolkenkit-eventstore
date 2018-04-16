'use strict';

const Eventstore = require('./dist/mariadb/Eventstore');

const eventstore = new Eventstore();

module.exports = eventstore;
