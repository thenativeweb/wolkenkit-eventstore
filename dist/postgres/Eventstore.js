'use strict';

var _regenerator = require('babel-runtime/regenerator');

var _regenerator2 = _interopRequireDefault(_regenerator);

var _asyncToGenerator2 = require('babel-runtime/helpers/asyncToGenerator');

var _asyncToGenerator3 = _interopRequireDefault(_asyncToGenerator2);

var _getPrototypeOf = require('babel-runtime/core-js/object/get-prototype-of');

var _getPrototypeOf2 = _interopRequireDefault(_getPrototypeOf);

var _classCallCheck2 = require('babel-runtime/helpers/classCallCheck');

var _classCallCheck3 = _interopRequireDefault(_classCallCheck2);

var _createClass2 = require('babel-runtime/helpers/createClass');

var _createClass3 = _interopRequireDefault(_createClass2);

var _possibleConstructorReturn2 = require('babel-runtime/helpers/possibleConstructorReturn');

var _possibleConstructorReturn3 = _interopRequireDefault(_possibleConstructorReturn2);

var _inherits2 = require('babel-runtime/helpers/inherits');

var _inherits3 = _interopRequireDefault(_inherits2);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

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

var Eventstore = function (_EventEmitter) {
  (0, _inherits3.default)(Eventstore, _EventEmitter);

  function Eventstore() {
    (0, _classCallCheck3.default)(this, Eventstore);
    return (0, _possibleConstructorReturn3.default)(this, (Eventstore.__proto__ || (0, _getPrototypeOf2.default)(Eventstore)).apply(this, arguments));
  }

  (0, _createClass3.default)(Eventstore, [{
    key: 'getDatabase',
    value: function () {
      var _ref = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee2() {
        var _this2 = this;

        var database;
        return _regenerator2.default.wrap(function _callee2$(_context2) {
          while (1) {
            switch (_context2.prev = _context2.next) {
              case 0:
                _context2.next = 2;
                return retry((0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee() {
                  var connection;
                  return _regenerator2.default.wrap(function _callee$(_context) {
                    while (1) {
                      switch (_context.prev = _context.next) {
                        case 0:
                          _context.next = 2;
                          return _this2.pool.connect();

                        case 2:
                          connection = _context.sent;
                          return _context.abrupt('return', connection);

                        case 4:
                        case 'end':
                          return _context.stop();
                      }
                    }
                  }, _callee, _this2);
                })));

              case 2:
                database = _context2.sent;
                return _context2.abrupt('return', database);

              case 4:
              case 'end':
                return _context2.stop();
            }
          }
        }, _callee2, this);
      }));

      function getDatabase() {
        return _ref.apply(this, arguments);
      }

      return getDatabase;
    }()
  }, {
    key: 'initialize',
    value: function () {
      var _ref4 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee4(_ref3) {
        var _this3 = this;

        var url = _ref3.url,
            namespace = _ref3.namespace;

        var _getParts, host, port, user, password, database, connection, disconnectWatcher;

        return _regenerator2.default.wrap(function _callee4$(_context4) {
          while (1) {
            switch (_context4.prev = _context4.next) {
              case 0:
                if (url) {
                  _context4.next = 2;
                  break;
                }

                throw new Error('Url is missing.');

              case 2:
                if (namespace) {
                  _context4.next = 4;
                  break;
                }

                throw new Error('Namespace is missing.');

              case 4:

                this.namespace = 'store_' + limitAlphanumeric(namespace);

                _getParts = new DsnParser(url).getParts(), host = _getParts.host, port = _getParts.port, user = _getParts.user, password = _getParts.password, database = _getParts.database;


                this.pool = new pg.Pool({ host: host, port: port, user: user, password: password, database: database });
                this.pool.on('error', function () {
                  _this3.emit('disconnect');
                });

                _context4.next = 10;
                return this.getDatabase();

              case 10:
                connection = _context4.sent;
                disconnectWatcher = new pg.Client({ host: host, port: port, user: user, password: password, database: database });


                disconnectWatcher.on('error', function () {
                  _this3.emit('disconnect');
                });
                disconnectWatcher.connect(function () {
                  disconnectWatcher.on('end', function () {
                    _this3.emit('disconnect');
                  });
                });

                _context4.prev = 14;
                _context4.next = 17;
                return retry((0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee3() {
                  return _regenerator2.default.wrap(function _callee3$(_context3) {
                    while (1) {
                      switch (_context3.prev = _context3.next) {
                        case 0:
                          _context3.next = 2;
                          return connection.query('\n          CREATE TABLE IF NOT EXISTS "' + _this3.namespace + '_events" (\n            "position" bigserial NOT NULL,\n            "aggregateId" uuid NOT NULL,\n            "revision" integer NOT NULL,\n            "event" jsonb NOT NULL,\n            "hasBeenPublished" boolean NOT NULL,\n\n            CONSTRAINT "' + _this3.namespace + '_events_pk" PRIMARY KEY("position"),\n            CONSTRAINT "' + _this3.namespace + '_aggregateId_revision" UNIQUE ("aggregateId", "revision")\n          );\n          CREATE TABLE IF NOT EXISTS "' + _this3.namespace + '_snapshots" (\n            "aggregateId" uuid NOT NULL,\n            "revision" integer NOT NULL,\n            "state" jsonb NOT NULL,\n\n            CONSTRAINT "' + _this3.namespace + '_snapshots_pk" PRIMARY KEY("aggregateId", "revision")\n          );\n        ');

                        case 2:
                        case 'end':
                          return _context3.stop();
                      }
                    }
                  }, _callee3, _this3);
                })), {
                  retries: 3,
                  minTimeout: 100,
                  factor: 1
                });

              case 17:
                _context4.prev = 17;

                connection.release();
                return _context4.finish(17);

              case 20:
              case 'end':
                return _context4.stop();
            }
          }
        }, _callee4, this, [[14,, 17, 20]]);
      }));

      function initialize(_x) {
        return _ref4.apply(this, arguments);
      }

      return initialize;
    }()
  }, {
    key: 'getLastEvent',
    value: function () {
      var _ref6 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee5(aggregateId) {
        var connection, result, event;
        return _regenerator2.default.wrap(function _callee5$(_context5) {
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
                  text: '\n          SELECT "event", "position"\n            FROM "' + this.namespace + '_events"\n            WHERE "aggregateId" = $1\n            ORDER BY "revision" DESC\n            LIMIT 1\n        ',
                  values: [aggregateId]
                });

              case 8:
                result = _context5.sent;

                if (!(result.rows.length === 0)) {
                  _context5.next = 11;
                  break;
                }

                return _context5.abrupt('return');

              case 11:
                event = Event.wrap(result.rows[0].event);


                event.metadata.position = Number(result.rows[0].position);

                return _context5.abrupt('return', event);

              case 14:
                _context5.prev = 14;

                connection.release();
                return _context5.finish(14);

              case 17:
              case 'end':
                return _context5.stop();
            }
          }
        }, _callee5, this, [[5,, 14, 17]]);
      }));

      function getLastEvent(_x2) {
        return _ref6.apply(this, arguments);
      }

      return getLastEvent;
    }()
  }, {
    key: 'getEventStream',
    value: function () {
      var _ref7 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee6(aggregateId, options) {
        var fromRevision, toRevision, connection, passThrough, eventStream, onData, onEnd, onError, unsubscribe;
        return _regenerator2.default.wrap(function _callee6$(_context6) {
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
                passThrough = new PassThrough({ objectMode: true });
                eventStream = connection.query(new QueryStream('\n        SELECT "event", "position", "hasBeenPublished"\n          FROM "' + this.namespace + '_events"\n          WHERE "aggregateId" = $1\n            AND "revision" >= $2\n            AND "revision" <= $3\n          ORDER BY "revision"', [aggregateId, fromRevision, toRevision]));
                onData = void 0, onEnd = void 0, onError = void 0;

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

                return _context6.abrupt('return', passThrough);

              case 21:
              case 'end':
                return _context6.stop();
            }
          }
        }, _callee6, this);
      }));

      function getEventStream(_x3, _x4) {
        return _ref7.apply(this, arguments);
      }

      return getEventStream;
    }()
  }, {
    key: 'getUnpublishedEventStream',
    value: function () {
      var _ref8 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee7() {
        var connection, passThrough, eventStream, onData, onEnd, onError, unsubscribe;
        return _regenerator2.default.wrap(function _callee7$(_context7) {
          while (1) {
            switch (_context7.prev = _context7.next) {
              case 0:
                _context7.next = 2;
                return this.getDatabase();

              case 2:
                connection = _context7.sent;
                passThrough = new PassThrough({ objectMode: true });
                eventStream = connection.query(new QueryStream('\n        SELECT "event", "position", "hasBeenPublished"\n          FROM "' + this.namespace + '_events"\n          WHERE "hasBeenPublished" = false\n          ORDER BY "position"'));
                onData = void 0, onEnd = void 0, onError = void 0;

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

                return _context7.abrupt('return', passThrough);

              case 14:
              case 'end':
                return _context7.stop();
            }
          }
        }, _callee7, this);
      }));

      function getUnpublishedEventStream() {
        return _ref8.apply(this, arguments);
      }

      return getUnpublishedEventStream;
    }()
  }, {
    key: 'saveEvents',
    value: function () {
      var _ref10 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee8(_ref9) {
        var events = _ref9.events;

        var placeholders, values, i, base, event, connection, text, result, _i;

        return _regenerator2.default.wrap(function _callee8$(_context8) {
          while (1) {
            switch (_context8.prev = _context8.next) {
              case 0:
                if (events) {
                  _context8.next = 2;
                  break;
                }

                throw new Error('Events are missing.');

              case 2:
                if (!(Array.isArray(events) && events.length === 0)) {
                  _context8.next = 4;
                  break;
                }

                throw new Error('Events are missing.');

              case 4:

                events = cloneDeep(flatten([events]));

                placeholders = [], values = [];
                i = 0;

              case 7:
                if (!(i < events.length)) {
                  _context8.next = 20;
                  break;
                }

                base = 4 * i + 1, event = events[i];

                if (event.metadata) {
                  _context8.next = 11;
                  break;
                }

                throw new Error('Metadata are missing.');

              case 11:
                if (!(event.metadata.revision === undefined)) {
                  _context8.next = 13;
                  break;
                }

                throw new Error('Revision is missing.');

              case 13:
                if (!(event.metadata.revision < 1)) {
                  _context8.next = 15;
                  break;
                }

                throw new Error('Revision must not be less than 1.');

              case 15:

                placeholders.push('($' + base + ', $' + (base + 1) + ', $' + (base + 2) + ', $' + (base + 3) + ')');
                values.push(event.aggregate.id, event.metadata.revision, event, event.metadata.published);

              case 17:
                i++;
                _context8.next = 7;
                break;

              case 20:
                _context8.next = 22;
                return this.getDatabase();

              case 22:
                connection = _context8.sent;
                text = '\n      INSERT INTO "' + this.namespace + '_events"\n        ("aggregateId", "revision", "event", "hasBeenPublished")\n      VALUES\n        ' + placeholders.join(',') + ' RETURNING position;\n    ';
                _context8.prev = 24;
                _context8.next = 27;
                return connection.query({ name: 'save events ' + events.length, text: text, values: values });

              case 27:
                result = _context8.sent;


                for (_i = 0; _i < result.rows.length; _i++) {
                  events[_i].metadata.position = Number(result.rows[_i].position);
                }

                return _context8.abrupt('return', events);

              case 32:
                _context8.prev = 32;
                _context8.t0 = _context8['catch'](24);

                if (!(_context8.t0.code === '23505' && _context8.t0.detail.startsWith('Key ("aggregateId", revision)'))) {
                  _context8.next = 36;
                  break;
                }

                throw new Error('Aggregate id and revision already exist.');

              case 36:
                throw _context8.t0;

              case 37:
                _context8.prev = 37;

                connection.release();
                return _context8.finish(37);

              case 40:
              case 'end':
                return _context8.stop();
            }
          }
        }, _callee8, this, [[24, 32, 37, 40]]);
      }));

      function saveEvents(_x5) {
        return _ref10.apply(this, arguments);
      }

      return saveEvents;
    }()
  }, {
    key: 'markEventsAsPublished',
    value: function () {
      var _ref12 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee9(_ref11) {
        var aggregateId = _ref11.aggregateId,
            fromRevision = _ref11.fromRevision,
            toRevision = _ref11.toRevision;
        var connection;
        return _regenerator2.default.wrap(function _callee9$(_context9) {
          while (1) {
            switch (_context9.prev = _context9.next) {
              case 0:
                if (aggregateId) {
                  _context9.next = 2;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 2:
                if (fromRevision) {
                  _context9.next = 4;
                  break;
                }

                throw new Error('From revision is missing.');

              case 4:
                if (toRevision) {
                  _context9.next = 6;
                  break;
                }

                throw new Error('To revision is missing.');

              case 6:
                if (!(fromRevision > toRevision)) {
                  _context9.next = 8;
                  break;
                }

                throw new Error('From revision is greater than to revision.');

              case 8:
                _context9.next = 10;
                return this.getDatabase();

              case 10:
                connection = _context9.sent;
                _context9.prev = 11;
                _context9.next = 14;
                return connection.query({
                  name: 'mark events as published',
                  text: '\n          UPDATE "' + this.namespace + '_events"\n            SET "hasBeenPublished" = true\n            WHERE "aggregateId" = $1\n              AND "revision" >= $2\n              AND "revision" <= $3\n        ',
                  values: [aggregateId, fromRevision, toRevision]
                });

              case 14:
                _context9.prev = 14;

                connection.release();
                return _context9.finish(14);

              case 17:
              case 'end':
                return _context9.stop();
            }
          }
        }, _callee9, this, [[11,, 14, 17]]);
      }));

      function markEventsAsPublished(_x6) {
        return _ref12.apply(this, arguments);
      }

      return markEventsAsPublished;
    }()
  }, {
    key: 'getSnapshot',
    value: function () {
      var _ref13 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee10(aggregateId) {
        var connection, result;
        return _regenerator2.default.wrap(function _callee10$(_context10) {
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
                  text: '\n          SELECT "state", "revision"\n            FROM "' + this.namespace + '_snapshots"\n            WHERE "aggregateId" = $1\n            ORDER BY "revision" DESC\n            LIMIT 1\n        ',
                  values: [aggregateId]
                });

              case 8:
                result = _context10.sent;

                if (!(result.rows.length === 0)) {
                  _context10.next = 11;
                  break;
                }

                return _context10.abrupt('return');

              case 11:
                return _context10.abrupt('return', {
                  revision: result.rows[0].revision,
                  state: result.rows[0].state
                });

              case 12:
                _context10.prev = 12;

                connection.release();
                return _context10.finish(12);

              case 15:
              case 'end':
                return _context10.stop();
            }
          }
        }, _callee10, this, [[5,, 12, 15]]);
      }));

      function getSnapshot(_x7) {
        return _ref13.apply(this, arguments);
      }

      return getSnapshot;
    }()
  }, {
    key: 'saveSnapshot',
    value: function () {
      var _ref15 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee11(_ref14) {
        var aggregateId = _ref14.aggregateId,
            revision = _ref14.revision,
            state = _ref14.state;
        var connection;
        return _regenerator2.default.wrap(function _callee11$(_context11) {
          while (1) {
            switch (_context11.prev = _context11.next) {
              case 0:
                if (aggregateId) {
                  _context11.next = 2;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 2:
                if (revision) {
                  _context11.next = 4;
                  break;
                }

                throw new Error('Revision is missing.');

              case 4:
                if (state) {
                  _context11.next = 6;
                  break;
                }

                throw new Error('State is missing.');

              case 6:

                state = omitByDeep(state, function (value) {
                  return value === undefined;
                });

                _context11.next = 9;
                return this.getDatabase();

              case 9:
                connection = _context11.sent;
                _context11.prev = 10;
                _context11.next = 13;
                return connection.query({
                  name: 'save snapshot',
                  text: '\n        INSERT INTO "' + this.namespace + '_snapshots" (\n          "aggregateId", revision, state\n        ) VALUES ($1, $2, $3)\n        ON CONFLICT DO NOTHING;\n        ',
                  values: [aggregateId, revision, state]
                });

              case 13:
                _context11.prev = 13;

                connection.release();
                return _context11.finish(13);

              case 16:
              case 'end':
                return _context11.stop();
            }
          }
        }, _callee11, this, [[10,, 13, 16]]);
      }));

      function saveSnapshot(_x8) {
        return _ref15.apply(this, arguments);
      }

      return saveSnapshot;
    }()
  }, {
    key: 'getReplay',
    value: function () {
      var _ref16 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee12(options) {
        var fromPosition, toPosition, connection, passThrough, eventStream, onData, onEnd, onError, unsubscribe;
        return _regenerator2.default.wrap(function _callee12$(_context12) {
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
                passThrough = new PassThrough({ objectMode: true });
                eventStream = connection.query(new QueryStream('\n        SELECT "event", "position"\n          FROM "' + this.namespace + '_events"\n          WHERE "position" >= $1\n            AND "position" <= $2\n          ORDER BY "position"', [fromPosition, toPosition]));
                onData = void 0, onEnd = void 0, onError = void 0;

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

                return _context12.abrupt('return', passThrough);

              case 19:
              case 'end':
                return _context12.stop();
            }
          }
        }, _callee12, this);
      }));

      function getReplay(_x9) {
        return _ref16.apply(this, arguments);
      }

      return getReplay;
    }()
  }, {
    key: 'destroy',
    value: function () {
      var _ref17 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee13() {
        return _regenerator2.default.wrap(function _callee13$(_context13) {
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
              case 'end':
                return _context13.stop();
            }
          }
        }, _callee13, this);
      }));

      function destroy() {
        return _ref17.apply(this, arguments);
      }

      return destroy;
    }()
  }]);
  return Eventstore;
}(EventEmitter);

module.exports = Eventstore;