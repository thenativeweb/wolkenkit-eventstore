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
    _require3 = require('commands-events'),
    Event = _require3.Event,
    flatten = require('lodash/flatten'),
    limitAlphanumeric = require('limit-alphanumeric'),
    _require4 = require('pg-connection-string'),
    parse = _require4.parse,
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
      var _ref = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee() {
        var database;
        return _regenerator2.default.wrap(function _callee$(_context) {
          while (1) {
            switch (_context.prev = _context.next) {
              case 0:
                _context.next = 2;
                return this.pool.connect();

              case 2:
                database = _context.sent;
                return _context.abrupt('return', database);

              case 4:
              case 'end':
                return _context.stop();
            }
          }
        }, _callee, this);
      }));

      function getDatabase() {
        return _ref.apply(this, arguments);
      }

      return getDatabase;
    }()
  }, {
    key: 'initialize',
    value: function () {
      var _ref3 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee3(_ref2) {
        var _this2 = this;

        var url = _ref2.url,
            namespace = _ref2.namespace;
        var disconnectWatcher, database;
        return _regenerator2.default.wrap(function _callee3$(_context3) {
          while (1) {
            switch (_context3.prev = _context3.next) {
              case 0:
                if (url) {
                  _context3.next = 2;
                  break;
                }

                throw new Error('Url is missing.');

              case 2:
                if (namespace) {
                  _context3.next = 4;
                  break;
                }

                throw new Error('Namespace is missing.');

              case 4:

                this.namespace = 'store_' + limitAlphanumeric(namespace);

                disconnectWatcher = new pg.Client(parse(url));


                disconnectWatcher.on('error', function () {
                  _this2.emit('disconnect');
                });
                disconnectWatcher.connect(function () {
                  disconnectWatcher.on('end', function () {
                    _this2.emit('disconnect');
                  });
                });

                this.pool = new pg.Pool(parse(url));
                this.pool.on('error', function () {
                  _this2.emit('disconnect');
                });

                _context3.next = 12;
                return this.getDatabase();

              case 12:
                database = _context3.sent;
                _context3.prev = 13;
                _context3.next = 16;
                return retry((0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee2() {
                  return _regenerator2.default.wrap(function _callee2$(_context2) {
                    while (1) {
                      switch (_context2.prev = _context2.next) {
                        case 0:
                          _context2.next = 2;
                          return database.query('\n          CREATE TABLE IF NOT EXISTS "' + _this2.namespace + '_events" (\n            "position" bigserial NOT NULL,\n            "aggregateId" uuid NOT NULL,\n            "revision" integer NOT NULL,\n            "event" jsonb NOT NULL,\n            "hasBeenPublished" boolean NOT NULL,\n\n            CONSTRAINT "' + _this2.namespace + '_events_pk" PRIMARY KEY("position"),\n            CONSTRAINT "' + _this2.namespace + '_aggregateId_revision" UNIQUE ("aggregateId", "revision")\n          );\n          CREATE TABLE IF NOT EXISTS "' + _this2.namespace + '_snapshots" (\n            "aggregateId" uuid NOT NULL,\n            "revision" integer NOT NULL,\n            "state" jsonb NOT NULL,\n\n            CONSTRAINT "' + _this2.namespace + '_snapshots_pk" PRIMARY KEY("aggregateId", "revision")\n          );\n        ');

                        case 2:
                        case 'end':
                          return _context2.stop();
                      }
                    }
                  }, _callee2, _this2);
                })), {
                  retries: 3,
                  minTimeout: 100,
                  factor: 1
                });

              case 16:
                _context3.prev = 16;

                database.release();
                return _context3.finish(16);

              case 19:
              case 'end':
                return _context3.stop();
            }
          }
        }, _callee3, this, [[13,, 16, 19]]);
      }));

      function initialize(_x) {
        return _ref3.apply(this, arguments);
      }

      return initialize;
    }()
  }, {
    key: 'getLastEvent',
    value: function () {
      var _ref5 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee4(aggregateId) {
        var database, result, event;
        return _regenerator2.default.wrap(function _callee4$(_context4) {
          while (1) {
            switch (_context4.prev = _context4.next) {
              case 0:
                if (aggregateId) {
                  _context4.next = 2;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 2:
                _context4.next = 4;
                return this.getDatabase();

              case 4:
                database = _context4.sent;
                _context4.prev = 5;
                _context4.next = 8;
                return database.query({
                  name: 'get last event',
                  text: '\n          SELECT "event", "position"\n            FROM "' + this.namespace + '_events"\n            WHERE "aggregateId" = $1\n            ORDER BY "revision" DESC\n            LIMIT 1\n        ',
                  values: [aggregateId]
                });

              case 8:
                result = _context4.sent;

                if (!(result.rows.length === 0)) {
                  _context4.next = 11;
                  break;
                }

                return _context4.abrupt('return');

              case 11:
                event = Event.wrap(result.rows[0].event);


                event.metadata.position = Number(result.rows[0].position);

                return _context4.abrupt('return', event);

              case 14:
                _context4.prev = 14;

                database.release();
                return _context4.finish(14);

              case 17:
              case 'end':
                return _context4.stop();
            }
          }
        }, _callee4, this, [[5,, 14, 17]]);
      }));

      function getLastEvent(_x2) {
        return _ref5.apply(this, arguments);
      }

      return getLastEvent;
    }()
  }, {
    key: 'getEventStream',
    value: function () {
      var _ref6 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee5(aggregateId, options) {
        var fromRevision, toRevision, database, passThrough, eventStream, onData, onEnd, onError, unsubscribe;
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

                options = options || {};

                fromRevision = options.fromRevision || 1;
                toRevision = options.toRevision || Math.pow(2, 31) - 1;

                if (!(fromRevision > toRevision)) {
                  _context5.next = 7;
                  break;
                }

                throw new Error('From revision is greater than to revision.');

              case 7:
                _context5.next = 9;
                return this.getDatabase();

              case 9:
                database = _context5.sent;
                passThrough = new PassThrough({ objectMode: true });
                eventStream = database.query(new QueryStream('\n        SELECT "event", "position", "hasBeenPublished"\n          FROM "' + this.namespace + '_events"\n          WHERE "aggregateId" = $1\n            AND "revision" >= $2\n            AND "revision" <= $3\n          ORDER BY "revision"', [aggregateId, fromRevision, toRevision]));
                onData = void 0, onEnd = void 0, onError = void 0;

                unsubscribe = function unsubscribe() {
                  database.release();
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

                return _context5.abrupt('return', passThrough);

              case 21:
              case 'end':
                return _context5.stop();
            }
          }
        }, _callee5, this);
      }));

      function getEventStream(_x3, _x4) {
        return _ref6.apply(this, arguments);
      }

      return getEventStream;
    }()
  }, {
    key: 'getUnpublishedEventStream',
    value: function () {
      var _ref7 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee6() {
        var database, passThrough, eventStream, onData, onEnd, onError, unsubscribe;
        return _regenerator2.default.wrap(function _callee6$(_context6) {
          while (1) {
            switch (_context6.prev = _context6.next) {
              case 0:
                _context6.next = 2;
                return this.getDatabase();

              case 2:
                database = _context6.sent;
                passThrough = new PassThrough({ objectMode: true });
                eventStream = database.query(new QueryStream('\n        SELECT "event", "position", "hasBeenPublished"\n          FROM "' + this.namespace + '_events"\n          WHERE "hasBeenPublished" = false\n          ORDER BY "position"'));
                onData = void 0, onEnd = void 0, onError = void 0;

                unsubscribe = function unsubscribe() {
                  database.release();
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

              case 14:
              case 'end':
                return _context6.stop();
            }
          }
        }, _callee6, this);
      }));

      function getUnpublishedEventStream() {
        return _ref7.apply(this, arguments);
      }

      return getUnpublishedEventStream;
    }()
  }, {
    key: 'saveEvents',
    value: function () {
      var _ref9 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee7(_ref8) {
        var events = _ref8.events;

        var database, placeholders, values, i, base, event, text, result, _i;

        return _regenerator2.default.wrap(function _callee7$(_context7) {
          while (1) {
            switch (_context7.prev = _context7.next) {
              case 0:
                if (events) {
                  _context7.next = 2;
                  break;
                }

                throw new Error('Events are missing.');

              case 2:

                events = cloneDeep(flatten([events]));

                _context7.next = 5;
                return this.getDatabase();

              case 5:
                database = _context7.sent;
                placeholders = [], values = [];


                for (i = 0; i < events.length; i++) {
                  base = 4 * i + 1, event = events[i];


                  placeholders.push('($' + base + ', $' + (base + 1) + ', $' + (base + 2) + ', $' + (base + 3) + ')');
                  values.push(event.aggregate.id, event.metadata.revision, event, event.metadata.published);
                }

                text = '\n      INSERT INTO "' + this.namespace + '_events"\n        ("aggregateId", "revision", "event", "hasBeenPublished")\n      VALUES\n        ' + placeholders.join(',') + ' RETURNING position;\n    ';
                _context7.prev = 9;
                _context7.next = 12;
                return database.query({ name: 'save events ' + events.length, text: text, values: values });

              case 12:
                result = _context7.sent;


                for (_i = 0; _i < result.rows.length; _i++) {
                  events[_i].metadata.position = Number(result.rows[_i].position);
                }

                return _context7.abrupt('return', events);

              case 17:
                _context7.prev = 17;
                _context7.t0 = _context7['catch'](9);

                if (!(_context7.t0.code === '23505' && _context7.t0.detail.startsWith('Key ("aggregateId", revision)'))) {
                  _context7.next = 21;
                  break;
                }

                throw new Error('Aggregate id and revision already exist.');

              case 21:
                throw _context7.t0;

              case 22:
                _context7.prev = 22;

                database.release();
                return _context7.finish(22);

              case 25:
              case 'end':
                return _context7.stop();
            }
          }
        }, _callee7, this, [[9, 17, 22, 25]]);
      }));

      function saveEvents(_x5) {
        return _ref9.apply(this, arguments);
      }

      return saveEvents;
    }()
  }, {
    key: 'markEventsAsPublished',
    value: function () {
      var _ref11 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee8(_ref10) {
        var aggregateId = _ref10.aggregateId,
            fromRevision = _ref10.fromRevision,
            toRevision = _ref10.toRevision;
        var database;
        return _regenerator2.default.wrap(function _callee8$(_context8) {
          while (1) {
            switch (_context8.prev = _context8.next) {
              case 0:
                if (aggregateId) {
                  _context8.next = 2;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 2:
                if (fromRevision) {
                  _context8.next = 4;
                  break;
                }

                throw new Error('From revision is missing.');

              case 4:
                if (toRevision) {
                  _context8.next = 6;
                  break;
                }

                throw new Error('To revision is missing.');

              case 6:
                if (!(fromRevision > toRevision)) {
                  _context8.next = 8;
                  break;
                }

                throw new Error('From revision is greater than to revision.');

              case 8:
                _context8.next = 10;
                return this.getDatabase();

              case 10:
                database = _context8.sent;
                _context8.prev = 11;
                _context8.next = 14;
                return database.query({
                  name: 'mark events as published',
                  text: '\n          UPDATE "' + this.namespace + '_events"\n            SET "hasBeenPublished" = true\n            WHERE "aggregateId" = $1\n              AND "revision" >= $2\n              AND "revision" <= $3\n        ',
                  values: [aggregateId, fromRevision, toRevision]
                });

              case 14:
                _context8.prev = 14;

                database.release();
                return _context8.finish(14);

              case 17:
              case 'end':
                return _context8.stop();
            }
          }
        }, _callee8, this, [[11,, 14, 17]]);
      }));

      function markEventsAsPublished(_x6) {
        return _ref11.apply(this, arguments);
      }

      return markEventsAsPublished;
    }()
  }, {
    key: 'getSnapshot',
    value: function () {
      var _ref12 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee9(aggregateId) {
        var database, result;
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
                _context9.next = 4;
                return this.getDatabase();

              case 4:
                database = _context9.sent;
                _context9.prev = 5;
                _context9.next = 8;
                return database.query({
                  name: 'get snapshot',
                  text: '\n          SELECT "state", "revision"\n            FROM "' + this.namespace + '_snapshots"\n            WHERE "aggregateId" = $1\n            ORDER BY "revision" DESC\n            LIMIT 1\n        ',
                  values: [aggregateId]
                });

              case 8:
                result = _context9.sent;

                if (!(result.rows.length === 0)) {
                  _context9.next = 11;
                  break;
                }

                return _context9.abrupt('return');

              case 11:
                return _context9.abrupt('return', {
                  revision: result.rows[0].revision,
                  state: result.rows[0].state
                });

              case 12:
                _context9.prev = 12;

                database.release();
                return _context9.finish(12);

              case 15:
              case 'end':
                return _context9.stop();
            }
          }
        }, _callee9, this, [[5,, 12, 15]]);
      }));

      function getSnapshot(_x7) {
        return _ref12.apply(this, arguments);
      }

      return getSnapshot;
    }()
  }, {
    key: 'saveSnapshot',
    value: function () {
      var _ref14 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee10(_ref13) {
        var aggregateId = _ref13.aggregateId,
            revision = _ref13.revision,
            state = _ref13.state;
        var database;
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
                if (revision) {
                  _context10.next = 4;
                  break;
                }

                throw new Error('Revision is missing.');

              case 4:
                if (state) {
                  _context10.next = 6;
                  break;
                }

                throw new Error('State is missing.');

              case 6:

                state = omitByDeep(state, function (value) {
                  return value === undefined;
                });

                _context10.next = 9;
                return this.getDatabase();

              case 9:
                database = _context10.sent;
                _context10.prev = 10;
                _context10.next = 13;
                return database.query({
                  name: 'save snapshot',
                  text: '\n        INSERT INTO "' + this.namespace + '_snapshots" (\n          "aggregateId", revision, state\n        ) VALUES ($1, $2, $3)\n        ON CONFLICT DO NOTHING;\n        ',
                  values: [aggregateId, revision, state]
                });

              case 13:
                _context10.prev = 13;

                database.release();
                return _context10.finish(13);

              case 16:
              case 'end':
                return _context10.stop();
            }
          }
        }, _callee10, this, [[10,, 13, 16]]);
      }));

      function saveSnapshot(_x8) {
        return _ref14.apply(this, arguments);
      }

      return saveSnapshot;
    }()
  }, {
    key: 'getReplay',
    value: function () {
      var _ref15 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee11(options) {
        var fromPosition, toPosition, database, passThrough, eventStream, onData, onEnd, onError, unsubscribe;
        return _regenerator2.default.wrap(function _callee11$(_context11) {
          while (1) {
            switch (_context11.prev = _context11.next) {
              case 0:
                options = options || {};

                fromPosition = options.fromPosition || 1;
                toPosition = options.toPosition || Math.pow(2, 31) - 1;

                if (!(fromPosition > toPosition)) {
                  _context11.next = 5;
                  break;
                }

                throw new Error('From position is greater than to position.');

              case 5:
                _context11.next = 7;
                return this.getDatabase();

              case 7:
                database = _context11.sent;
                passThrough = new PassThrough({ objectMode: true });
                eventStream = database.query(new QueryStream('\n        SELECT "event", "position"\n          FROM "' + this.namespace + '_events"\n          WHERE "position" >= $1\n            AND "position" <= $2\n          ORDER BY "position"', [fromPosition, toPosition]));
                onData = void 0, onEnd = void 0, onError = void 0;

                unsubscribe = function unsubscribe() {
                  database.release();
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

                return _context11.abrupt('return', passThrough);

              case 19:
              case 'end':
                return _context11.stop();
            }
          }
        }, _callee11, this);
      }));

      function getReplay(_x9) {
        return _ref15.apply(this, arguments);
      }

      return getReplay;
    }()
  }, {
    key: 'destroy',
    value: function () {
      var _ref16 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee12() {
        return _regenerator2.default.wrap(function _callee12$(_context12) {
          while (1) {
            switch (_context12.prev = _context12.next) {
              case 0:
                if (!this.pool) {
                  _context12.next = 3;
                  break;
                }

                _context12.next = 3;
                return this.pool.end();

              case 3:
              case 'end':
                return _context12.stop();
            }
          }
        }, _callee12, this);
      }));

      function destroy() {
        return _ref16.apply(this, arguments);
      }

      return destroy;
    }()
  }]);
  return Eventstore;
}(EventEmitter);

module.exports = Eventstore;