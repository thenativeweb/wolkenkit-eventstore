'use strict';

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _regenerator = _interopRequireDefault(require("@babel/runtime/regenerator"));

var _asyncToGenerator2 = _interopRequireDefault(require("@babel/runtime/helpers/asyncToGenerator"));

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _require = require('events'),
    EventEmitter = _require.EventEmitter,
    _require2 = require('stream'),
    PassThrough = _require2.PassThrough;

var cloneDeep = require('lodash/cloneDeep'),
    DsnParser = require('dsn-parser'),
    _require3 = require('commands-events'),
    Event = _require3.Event,
    flatten = require('lodash/flatten'),
    limitAlphanumeric = require('limit-alphanumeric'),
    pg = require('pg'),
    QueryStream = require('pg-query-stream'),
    retry = require('async-retry');

var omitByDeep = require('../omitByDeep');

var Eventstore =
/*#__PURE__*/
function (_EventEmitter) {
  (0, _inherits2.default)(Eventstore, _EventEmitter);

  function Eventstore() {
    (0, _classCallCheck2.default)(this, Eventstore);
    return (0, _possibleConstructorReturn2.default)(this, (0, _getPrototypeOf2.default)(Eventstore).apply(this, arguments));
  }

  (0, _createClass2.default)(Eventstore, [{
    key: "getDatabase",
    value: function () {
      var _getDatabase = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee2() {
        var _this = this;

        var database;
        return _regenerator.default.wrap(function _callee2$(_context2) {
          while (1) {
            switch (_context2.prev = _context2.next) {
              case 0:
                _context2.next = 2;
                return retry(
                /*#__PURE__*/
                (0, _asyncToGenerator2.default)(
                /*#__PURE__*/
                _regenerator.default.mark(function _callee() {
                  var connection;
                  return _regenerator.default.wrap(function _callee$(_context) {
                    while (1) {
                      switch (_context.prev = _context.next) {
                        case 0:
                          _context.next = 2;
                          return _this.pool.connect();

                        case 2:
                          connection = _context.sent;
                          return _context.abrupt("return", connection);

                        case 4:
                        case "end":
                          return _context.stop();
                      }
                    }
                  }, _callee, this);
                })));

              case 2:
                database = _context2.sent;
                return _context2.abrupt("return", database);

              case 4:
              case "end":
                return _context2.stop();
            }
          }
        }, _callee2, this);
      }));

      function getDatabase() {
        return _getDatabase.apply(this, arguments);
      }

      return getDatabase;
    }()
  }, {
    key: "initialize",
    value: function () {
      var _initialize = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee4(_ref2) {
        var _this2 = this;

        var url, namespace, _getParts, host, port, user, password, database, connection;

        return _regenerator.default.wrap(function _callee4$(_context4) {
          while (1) {
            switch (_context4.prev = _context4.next) {
              case 0:
                url = _ref2.url, namespace = _ref2.namespace;

                if (url) {
                  _context4.next = 3;
                  break;
                }

                throw new Error('Url is missing.');

              case 3:
                if (namespace) {
                  _context4.next = 5;
                  break;
                }

                throw new Error('Namespace is missing.');

              case 5:
                this.namespace = "store_".concat(limitAlphanumeric(namespace));
                _getParts = new DsnParser(url).getParts(), host = _getParts.host, port = _getParts.port, user = _getParts.user, password = _getParts.password, database = _getParts.database;
                this.pool = new pg.Pool({
                  host: host,
                  port: port,
                  user: user,
                  password: password,
                  database: database
                });
                this.pool.on('error', function () {
                  _this2.emit('disconnect');
                });
                _context4.next = 11;
                return this.getDatabase();

              case 11:
                connection = _context4.sent;
                this.disconnectWatcher = new pg.Client({
                  host: host,
                  port: port,
                  user: user,
                  password: password,
                  database: database
                });
                this.disconnectWatcher.on('error', function () {
                  _this2.emit('disconnect');
                });
                this.disconnectWatcher.connect(function () {
                  _this2.disconnectWatcher.on('end', function () {
                    _this2.emit('disconnect');
                  });
                });
                _context4.prev = 15;
                _context4.next = 18;
                return retry(
                /*#__PURE__*/
                (0, _asyncToGenerator2.default)(
                /*#__PURE__*/
                _regenerator.default.mark(function _callee3() {
                  return _regenerator.default.wrap(function _callee3$(_context3) {
                    while (1) {
                      switch (_context3.prev = _context3.next) {
                        case 0:
                          _context3.next = 2;
                          return connection.query("\n          CREATE TABLE IF NOT EXISTS \"".concat(_this2.namespace, "_events\" (\n            \"position\" bigserial NOT NULL,\n            \"aggregateId\" uuid NOT NULL,\n            \"revision\" integer NOT NULL,\n            \"event\" jsonb NOT NULL,\n            \"hasBeenPublished\" boolean NOT NULL,\n\n            CONSTRAINT \"").concat(_this2.namespace, "_events_pk\" PRIMARY KEY(\"position\"),\n            CONSTRAINT \"").concat(_this2.namespace, "_aggregateId_revision\" UNIQUE (\"aggregateId\", \"revision\")\n          );\n          CREATE TABLE IF NOT EXISTS \"").concat(_this2.namespace, "_snapshots\" (\n            \"aggregateId\" uuid NOT NULL,\n            \"revision\" integer NOT NULL,\n            \"state\" jsonb NOT NULL,\n\n            CONSTRAINT \"").concat(_this2.namespace, "_snapshots_pk\" PRIMARY KEY(\"aggregateId\", \"revision\")\n          );\n        "));

                        case 2:
                        case "end":
                          return _context3.stop();
                      }
                    }
                  }, _callee3, this);
                })), {
                  retries: 3,
                  minTimeout: 100,
                  factor: 1
                });

              case 18:
                _context4.prev = 18;
                connection.release();
                return _context4.finish(18);

              case 21:
              case "end":
                return _context4.stop();
            }
          }
        }, _callee4, this, [[15,, 18, 21]]);
      }));

      function initialize(_x) {
        return _initialize.apply(this, arguments);
      }

      return initialize;
    }()
  }, {
    key: "getLastEvent",
    value: function () {
      var _getLastEvent = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee5(aggregateId) {
        var connection, result, event;
        return _regenerator.default.wrap(function _callee5$(_context5) {
          while (1) {
            switch (_context5.prev = _context5.next) {
              case 0:
                if (aggregateId) {
                  _context5.next = 2;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 2:
                _context5.next = 4;
                return this.getDatabase();

              case 4:
                connection = _context5.sent;
                _context5.prev = 5;
                _context5.next = 8;
                return connection.query({
                  name: 'get last event',
                  text: "\n          SELECT \"event\", \"position\"\n            FROM \"".concat(this.namespace, "_events\"\n            WHERE \"aggregateId\" = $1\n            ORDER BY \"revision\" DESC\n            LIMIT 1\n        "),
                  values: [aggregateId]
                });

              case 8:
                result = _context5.sent;

                if (!(result.rows.length === 0)) {
                  _context5.next = 11;
                  break;
                }

                return _context5.abrupt("return");

              case 11:
                event = Event.wrap(result.rows[0].event);
                event.metadata.position = Number(result.rows[0].position);
                return _context5.abrupt("return", event);

              case 14:
                _context5.prev = 14;
                connection.release();
                return _context5.finish(14);

              case 17:
              case "end":
                return _context5.stop();
            }
          }
        }, _callee5, this, [[5,, 14, 17]]);
      }));

      function getLastEvent(_x2) {
        return _getLastEvent.apply(this, arguments);
      }

      return getLastEvent;
    }()
  }, {
    key: "getEventStream",
    value: function () {
      var _getEventStream = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee6(aggregateId, options) {
        var fromRevision, toRevision, connection, passThrough, eventStream, onData, onEnd, onError, unsubscribe;
        return _regenerator.default.wrap(function _callee6$(_context6) {
          while (1) {
            switch (_context6.prev = _context6.next) {
              case 0:
                if (aggregateId) {
                  _context6.next = 2;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 2:
                options = options || {};
                fromRevision = options.fromRevision || 1;
                toRevision = options.toRevision || Math.pow(2, 31) - 1;

                if (!(fromRevision > toRevision)) {
                  _context6.next = 7;
                  break;
                }

                throw new Error('From revision is greater than to revision.');

              case 7:
                _context6.next = 9;
                return this.getDatabase();

              case 9:
                connection = _context6.sent;
                passThrough = new PassThrough({
                  objectMode: true
                });
                eventStream = connection.query(new QueryStream("\n        SELECT \"event\", \"position\", \"hasBeenPublished\"\n          FROM \"".concat(this.namespace, "_events\"\n          WHERE \"aggregateId\" = $1\n            AND \"revision\" >= $2\n            AND \"revision\" <= $3\n          ORDER BY \"revision\""), [aggregateId, fromRevision, toRevision]));

                unsubscribe = function unsubscribe() {
                  connection.release();
                  eventStream.removeListener('data', onData);
                  eventStream.removeListener('end', onEnd);
                  eventStream.removeListener('error', onError);
                };

                onData = function onData(data) {
                  var event = Event.wrap(data.event);
                  event.metadata.position = Number(data.position);
                  event.metadata.published = data.hasBeenPublished;
                  passThrough.write(event);
                };

                onEnd = function onEnd() {
                  unsubscribe();
                  passThrough.end();
                };

                onError = function onError(err) {
                  unsubscribe();
                  passThrough.emit('error', err);
                  passThrough.end();
                };

                eventStream.on('data', onData);
                eventStream.on('end', onEnd);
                eventStream.on('error', onError);
                return _context6.abrupt("return", passThrough);

              case 20:
              case "end":
                return _context6.stop();
            }
          }
        }, _callee6, this);
      }));

      function getEventStream(_x3, _x4) {
        return _getEventStream.apply(this, arguments);
      }

      return getEventStream;
    }()
  }, {
    key: "getUnpublishedEventStream",
    value: function () {
      var _getUnpublishedEventStream = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee7() {
        var connection, passThrough, eventStream, onData, onEnd, onError, unsubscribe;
        return _regenerator.default.wrap(function _callee7$(_context7) {
          while (1) {
            switch (_context7.prev = _context7.next) {
              case 0:
                _context7.next = 2;
                return this.getDatabase();

              case 2:
                connection = _context7.sent;
                passThrough = new PassThrough({
                  objectMode: true
                });
                eventStream = connection.query(new QueryStream("\n        SELECT \"event\", \"position\", \"hasBeenPublished\"\n          FROM \"".concat(this.namespace, "_events\"\n          WHERE \"hasBeenPublished\" = false\n          ORDER BY \"position\"")));

                unsubscribe = function unsubscribe() {
                  connection.release();
                  eventStream.removeListener('data', onData);
                  eventStream.removeListener('end', onEnd);
                  eventStream.removeListener('error', onError);
                };

                onData = function onData(data) {
                  var event = Event.wrap(data.event);
                  event.metadata.position = Number(data.position);
                  event.metadata.published = data.hasBeenPublished;
                  passThrough.write(event);
                };

                onEnd = function onEnd() {
                  unsubscribe();
                  passThrough.end();
                };

                onError = function onError(err) {
                  unsubscribe();
                  passThrough.emit('error', err);
                  passThrough.end();
                };

                eventStream.on('data', onData);
                eventStream.on('end', onEnd);
                eventStream.on('error', onError);
                return _context7.abrupt("return", passThrough);

              case 13:
              case "end":
                return _context7.stop();
            }
          }
        }, _callee7, this);
      }));

      function getUnpublishedEventStream() {
        return _getUnpublishedEventStream.apply(this, arguments);
      }

      return getUnpublishedEventStream;
    }()
  }, {
    key: "saveEvents",
    value: function () {
      var _saveEvents = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee8(_ref4) {
        var events, placeholders, values, i, base, event, connection, text, result, _i;

        return _regenerator.default.wrap(function _callee8$(_context8) {
          while (1) {
            switch (_context8.prev = _context8.next) {
              case 0:
                events = _ref4.events;

                if (events) {
                  _context8.next = 3;
                  break;
                }

                throw new Error('Events are missing.');

              case 3:
                if (!(Array.isArray(events) && events.length === 0)) {
                  _context8.next = 5;
                  break;
                }

                throw new Error('Events are missing.');

              case 5:
                events = cloneDeep(flatten([events]));
                placeholders = [], values = [];
                i = 0;

              case 8:
                if (!(i < events.length)) {
                  _context8.next = 21;
                  break;
                }

                base = 4 * i + 1, event = events[i];

                if (event.metadata) {
                  _context8.next = 12;
                  break;
                }

                throw new Error('Metadata are missing.');

              case 12:
                if (!(event.metadata.revision === undefined)) {
                  _context8.next = 14;
                  break;
                }

                throw new Error('Revision is missing.');

              case 14:
                if (!(event.metadata.revision < 1)) {
                  _context8.next = 16;
                  break;
                }

                throw new Error('Revision must not be less than 1.');

              case 16:
                placeholders.push("($".concat(base, ", $").concat(base + 1, ", $").concat(base + 2, ", $").concat(base + 3, ")"));
                values.push(event.aggregate.id, event.metadata.revision, event, event.metadata.published);

              case 18:
                i++;
                _context8.next = 8;
                break;

              case 21:
                _context8.next = 23;
                return this.getDatabase();

              case 23:
                connection = _context8.sent;
                text = "\n      INSERT INTO \"".concat(this.namespace, "_events\"\n        (\"aggregateId\", \"revision\", \"event\", \"hasBeenPublished\")\n      VALUES\n        ").concat(placeholders.join(','), " RETURNING position;\n    ");
                _context8.prev = 25;
                _context8.next = 28;
                return connection.query({
                  name: "save events ".concat(events.length),
                  text: text,
                  values: values
                });

              case 28:
                result = _context8.sent;

                for (_i = 0; _i < result.rows.length; _i++) {
                  events[_i].metadata.position = Number(result.rows[_i].position);
                }

                return _context8.abrupt("return", events);

              case 33:
                _context8.prev = 33;
                _context8.t0 = _context8["catch"](25);

                if (!(_context8.t0.code === '23505' && _context8.t0.detail.startsWith('Key ("aggregateId", revision)'))) {
                  _context8.next = 37;
                  break;
                }

                throw new Error('Aggregate id and revision already exist.');

              case 37:
                throw _context8.t0;

              case 38:
                _context8.prev = 38;
                connection.release();
                return _context8.finish(38);

              case 41:
              case "end":
                return _context8.stop();
            }
          }
        }, _callee8, this, [[25, 33, 38, 41]]);
      }));

      function saveEvents(_x5) {
        return _saveEvents.apply(this, arguments);
      }

      return saveEvents;
    }()
  }, {
    key: "markEventsAsPublished",
    value: function () {
      var _markEventsAsPublished = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee9(_ref5) {
        var aggregateId, fromRevision, toRevision, connection;
        return _regenerator.default.wrap(function _callee9$(_context9) {
          while (1) {
            switch (_context9.prev = _context9.next) {
              case 0:
                aggregateId = _ref5.aggregateId, fromRevision = _ref5.fromRevision, toRevision = _ref5.toRevision;

                if (aggregateId) {
                  _context9.next = 3;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 3:
                if (fromRevision) {
                  _context9.next = 5;
                  break;
                }

                throw new Error('From revision is missing.');

              case 5:
                if (toRevision) {
                  _context9.next = 7;
                  break;
                }

                throw new Error('To revision is missing.');

              case 7:
                if (!(fromRevision > toRevision)) {
                  _context9.next = 9;
                  break;
                }

                throw new Error('From revision is greater than to revision.');

              case 9:
                _context9.next = 11;
                return this.getDatabase();

              case 11:
                connection = _context9.sent;
                _context9.prev = 12;
                _context9.next = 15;
                return connection.query({
                  name: 'mark events as published',
                  text: "\n          UPDATE \"".concat(this.namespace, "_events\"\n            SET \"hasBeenPublished\" = true\n            WHERE \"aggregateId\" = $1\n              AND \"revision\" >= $2\n              AND \"revision\" <= $3\n        "),
                  values: [aggregateId, fromRevision, toRevision]
                });

              case 15:
                _context9.prev = 15;
                connection.release();
                return _context9.finish(15);

              case 18:
              case "end":
                return _context9.stop();
            }
          }
        }, _callee9, this, [[12,, 15, 18]]);
      }));

      function markEventsAsPublished(_x6) {
        return _markEventsAsPublished.apply(this, arguments);
      }

      return markEventsAsPublished;
    }()
  }, {
    key: "getSnapshot",
    value: function () {
      var _getSnapshot = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee10(aggregateId) {
        var connection, result;
        return _regenerator.default.wrap(function _callee10$(_context10) {
          while (1) {
            switch (_context10.prev = _context10.next) {
              case 0:
                if (aggregateId) {
                  _context10.next = 2;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 2:
                _context10.next = 4;
                return this.getDatabase();

              case 4:
                connection = _context10.sent;
                _context10.prev = 5;
                _context10.next = 8;
                return connection.query({
                  name: 'get snapshot',
                  text: "\n          SELECT \"state\", \"revision\"\n            FROM \"".concat(this.namespace, "_snapshots\"\n            WHERE \"aggregateId\" = $1\n            ORDER BY \"revision\" DESC\n            LIMIT 1\n        "),
                  values: [aggregateId]
                });

              case 8:
                result = _context10.sent;

                if (!(result.rows.length === 0)) {
                  _context10.next = 11;
                  break;
                }

                return _context10.abrupt("return");

              case 11:
                return _context10.abrupt("return", {
                  revision: result.rows[0].revision,
                  state: result.rows[0].state
                });

              case 12:
                _context10.prev = 12;
                connection.release();
                return _context10.finish(12);

              case 15:
              case "end":
                return _context10.stop();
            }
          }
        }, _callee10, this, [[5,, 12, 15]]);
      }));

      function getSnapshot(_x7) {
        return _getSnapshot.apply(this, arguments);
      }

      return getSnapshot;
    }()
  }, {
    key: "saveSnapshot",
    value: function () {
      var _saveSnapshot = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee11(_ref6) {
        var aggregateId, revision, state, connection;
        return _regenerator.default.wrap(function _callee11$(_context11) {
          while (1) {
            switch (_context11.prev = _context11.next) {
              case 0:
                aggregateId = _ref6.aggregateId, revision = _ref6.revision, state = _ref6.state;

                if (aggregateId) {
                  _context11.next = 3;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 3:
                if (revision) {
                  _context11.next = 5;
                  break;
                }

                throw new Error('Revision is missing.');

              case 5:
                if (state) {
                  _context11.next = 7;
                  break;
                }

                throw new Error('State is missing.');

              case 7:
                state = omitByDeep(state, function (value) {
                  return value === undefined;
                });
                _context11.next = 10;
                return this.getDatabase();

              case 10:
                connection = _context11.sent;
                _context11.prev = 11;
                _context11.next = 14;
                return connection.query({
                  name: 'save snapshot',
                  text: "\n        INSERT INTO \"".concat(this.namespace, "_snapshots\" (\n          \"aggregateId\", revision, state\n        ) VALUES ($1, $2, $3)\n        ON CONFLICT DO NOTHING;\n        "),
                  values: [aggregateId, revision, state]
                });

              case 14:
                _context11.prev = 14;
                connection.release();
                return _context11.finish(14);

              case 17:
              case "end":
                return _context11.stop();
            }
          }
        }, _callee11, this, [[11,, 14, 17]]);
      }));

      function saveSnapshot(_x8) {
        return _saveSnapshot.apply(this, arguments);
      }

      return saveSnapshot;
    }()
  }, {
    key: "getReplay",
    value: function () {
      var _getReplay = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee12(options) {
        var fromPosition, toPosition, connection, passThrough, eventStream, onData, onEnd, onError, unsubscribe;
        return _regenerator.default.wrap(function _callee12$(_context12) {
          while (1) {
            switch (_context12.prev = _context12.next) {
              case 0:
                options = options || {};
                fromPosition = options.fromPosition || 1;
                toPosition = options.toPosition || Math.pow(2, 31) - 1;

                if (!(fromPosition > toPosition)) {
                  _context12.next = 5;
                  break;
                }

                throw new Error('From position is greater than to position.');

              case 5:
                _context12.next = 7;
                return this.getDatabase();

              case 7:
                connection = _context12.sent;
                passThrough = new PassThrough({
                  objectMode: true
                });
                eventStream = connection.query(new QueryStream("\n        SELECT \"event\", \"position\"\n          FROM \"".concat(this.namespace, "_events\"\n          WHERE \"position\" >= $1\n            AND \"position\" <= $2\n          ORDER BY \"position\""), [fromPosition, toPosition]));

                unsubscribe = function unsubscribe() {
                  connection.release();
                  eventStream.removeListener('data', onData);
                  eventStream.removeListener('end', onEnd);
                  eventStream.removeListener('error', onError);
                };

                onData = function onData(data) {
                  var event = Event.wrap(data.event);
                  event.metadata.position = Number(data.position);
                  passThrough.write(event);
                };

                onEnd = function onEnd() {
                  unsubscribe();
                  passThrough.end();
                };

                onError = function onError(err) {
                  unsubscribe();
                  passThrough.emit('error', err);
                  passThrough.end();
                };

                eventStream.on('data', onData);
                eventStream.on('end', onEnd);
                eventStream.on('error', onError);
                return _context12.abrupt("return", passThrough);

              case 18:
              case "end":
                return _context12.stop();
            }
          }
        }, _callee12, this);
      }));

      function getReplay(_x9) {
        return _getReplay.apply(this, arguments);
      }

      return getReplay;
    }()
  }, {
    key: "destroy",
    value: function () {
      var _destroy = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee13() {
        return _regenerator.default.wrap(function _callee13$(_context13) {
          while (1) {
            switch (_context13.prev = _context13.next) {
              case 0:
                if (!this.pool) {
                  _context13.next = 3;
                  break;
                }

                _context13.next = 3;
                return this.pool.end();

              case 3:
                if (!this.disconnectWatcher) {
                  _context13.next = 6;
                  break;
                }

                _context13.next = 6;
                return this.disconnectWatcher.end();

              case 6:
              case "end":
                return _context13.stop();
            }
          }
        }, _callee13, this);
      }));

      function destroy() {
        return _destroy.apply(this, arguments);
      }

      return destroy;
    }()
  }]);
  return Eventstore;
}(EventEmitter);

module.exports = Eventstore;