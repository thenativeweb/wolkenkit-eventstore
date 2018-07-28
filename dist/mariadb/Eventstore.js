'use strict';

var _stringify = require('babel-runtime/core-js/json/stringify');

var _stringify2 = _interopRequireDefault(_stringify);

var _slicedToArray2 = require('babel-runtime/helpers/slicedToArray');

var _slicedToArray3 = _interopRequireDefault(_slicedToArray2);

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
    mysql = require('mysql2/promise'),
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
                          return _this2.pool.getConnection();

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
      var _ref4 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee3(_ref3) {
        var _this3 = this;

        var url = _ref3.url,
            namespace = _ref3.namespace;

        var _getParts, host, port, user, password, database, connection, query;

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

                _getParts = new DsnParser(url).getParts(), host = _getParts.host, port = _getParts.port, user = _getParts.user, password = _getParts.password, database = _getParts.database;


                this.pool = mysql.createPool({
                  host: host,
                  port: port,
                  user: user,
                  password: password,
                  database: database,
                  multipleStatements: true
                });

                this.pool.on('connection', function (connection) {
                  connection.on('error', function () {
                    _this3.emit('disconnect');
                  });
                  connection.on('end', function () {
                    _this3.emit('disconnect');
                  });
                });

                _context3.next = 10;
                return this.getDatabase();

              case 10:
                connection = _context3.sent;
                query = '\n      CREATE FUNCTION IF NOT EXISTS UuidToBin(_uuid BINARY(36))\n        RETURNS BINARY(16)\n        RETURN UNHEX(CONCAT(\n          SUBSTR(_uuid, 15, 4),\n          SUBSTR(_uuid, 10, 4),\n          SUBSTR(_uuid, 1, 8),\n          SUBSTR(_uuid, 20, 4),\n          SUBSTR(_uuid, 25)\n        ));\n\n      CREATE FUNCTION IF NOT EXISTS UuidFromBin(_bin BINARY(16))\n        RETURNS BINARY(36)\n        RETURN LCASE(CONCAT_WS(\'-\',\n          HEX(SUBSTR(_bin,  5, 4)),\n          HEX(SUBSTR(_bin,  3, 2)),\n          HEX(SUBSTR(_bin,  1, 2)),\n          HEX(SUBSTR(_bin,  9, 2)),\n          HEX(SUBSTR(_bin, 11))\n        ));\n\n      CREATE TABLE IF NOT EXISTS ' + this.namespace + '_events (\n        position SERIAL,\n        aggregateId BINARY(16) NOT NULL,\n        revision INT NOT NULL,\n        event JSON NOT NULL,\n        hasBeenPublished BOOLEAN NOT NULL,\n\n        PRIMARY KEY(position),\n        UNIQUE (aggregateId, revision)\n      ) ENGINE=InnoDB;\n\n      CREATE TABLE IF NOT EXISTS ' + this.namespace + '_snapshots (\n        aggregateId BINARY(16) NOT NULL,\n        revision INT NOT NULL,\n        state JSON NOT NULL,\n\n        PRIMARY KEY(aggregateId, revision)\n      ) ENGINE=InnoDB;\n    ';
                _context3.next = 14;
                return connection.query(query);

              case 14:
                _context3.next = 16;
                return connection.release();

              case 16:
              case 'end':
                return _context3.stop();
            }
          }
        }, _callee3, this);
      }));

      function initialize(_x) {
        return _ref4.apply(this, arguments);
      }

      return initialize;
    }()
  }, {
    key: 'getLastEvent',
    value: function () {
      var _ref5 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee4(aggregateId) {
        var connection, _ref6, _ref7, rows, event;

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
                connection = _context4.sent;
                _context4.prev = 5;
                _context4.next = 8;
                return connection.execute('\n        SELECT event, position\n          FROM ' + this.namespace + '_events\n          WHERE aggregateId = UuidToBin(?)\n          ORDER BY revision DESC\n          LIMIT 1\n        ', [aggregateId]);

              case 8:
                _ref6 = _context4.sent;
                _ref7 = (0, _slicedToArray3.default)(_ref6, 1);
                rows = _ref7[0];

                if (!(rows.length === 0)) {
                  _context4.next = 13;
                  break;
                }

                return _context4.abrupt('return');

              case 13:
                event = Event.wrap(JSON.parse(rows[0].event));


                event.metadata.position = Number(rows[0].position);

                return _context4.abrupt('return', event);

              case 16:
                _context4.prev = 16;
                _context4.next = 19;
                return connection.release();

              case 19:
                return _context4.finish(16);

              case 20:
              case 'end':
                return _context4.stop();
            }
          }
        }, _callee4, this, [[5,, 16, 20]]);
      }));

      function getLastEvent(_x2) {
        return _ref5.apply(this, arguments);
      }

      return getLastEvent;
    }()
  }, {
    key: 'getEventStream',
    value: function () {
      var _ref8 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee5(aggregateId, options) {
        var fromRevision, toRevision, connection, passThrough, eventStream, onEnd, onError, onResult, unsubscribe;
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
                connection = _context5.sent;
                passThrough = new PassThrough({ objectMode: true });
                eventStream = connection.connection.execute('\n      SELECT event, position, hasBeenPublished\n        FROM ' + this.namespace + '_events\n        WHERE aggregateId = UuidToBin(?)\n          AND revision >= ?\n          AND revision <= ?\n        ORDER BY revision', [aggregateId, fromRevision, toRevision]);
                onEnd = void 0, onError = void 0, onResult = void 0;

                unsubscribe = function unsubscribe() {
                  connection.release();
                  eventStream.removeListener('end', onEnd);
                  eventStream.removeListener('error', onError);
                  eventStream.removeListener('result', onResult);
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

                onResult = function onResult(row) {
                  var event = Event.wrap(JSON.parse(row.event));

                  event.metadata.position = Number(row.position);
                  event.metadata.published = Boolean(row.hasBeenPublished);

                  passThrough.write(event);
                };

                eventStream.on('end', onEnd);
                eventStream.on('error', onError);
                eventStream.on('result', onResult);

                return _context5.abrupt('return', passThrough);

              case 21:
              case 'end':
                return _context5.stop();
            }
          }
        }, _callee5, this);
      }));

      function getEventStream(_x3, _x4) {
        return _ref8.apply(this, arguments);
      }

      return getEventStream;
    }()
  }, {
    key: 'getUnpublishedEventStream',
    value: function () {
      var _ref9 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee6() {
        var connection, passThrough, eventStream, onEnd, onError, onResult, unsubscribe;
        return _regenerator2.default.wrap(function _callee6$(_context6) {
          while (1) {
            switch (_context6.prev = _context6.next) {
              case 0:
                _context6.next = 2;
                return this.getDatabase();

              case 2:
                connection = _context6.sent;
                passThrough = new PassThrough({ objectMode: true });
                eventStream = connection.connection.execute('\n      SELECT event, position, hasBeenPublished\n        FROM ' + this.namespace + '_events\n        WHERE hasBeenPublished = false\n        ORDER BY position\n    ');
                onEnd = void 0, onError = void 0, onResult = void 0;

                unsubscribe = function unsubscribe() {
                  connection.release();
                  eventStream.removeListener('end', onEnd);
                  eventStream.removeListener('error', onError);
                  eventStream.removeListener('result', onResult);
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

                onResult = function onResult(row) {
                  var event = Event.wrap(JSON.parse(row.event));

                  event.metadata.position = Number(row.position);
                  event.metadata.published = Boolean(row.hasBeenPublished);
                  passThrough.write(event);
                };

                eventStream.on('end', onEnd);
                eventStream.on('error', onError);
                eventStream.on('result', onResult);

                return _context6.abrupt('return', passThrough);

              case 14:
              case 'end':
                return _context6.stop();
            }
          }
        }, _callee6, this);
      }));

      function getUnpublishedEventStream() {
        return _ref9.apply(this, arguments);
      }

      return getUnpublishedEventStream;
    }()
  }, {
    key: 'saveEvents',
    value: function () {
      var _ref11 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee7(_ref10) {
        var events = _ref10.events;

        var connection, placeholders, values, i, event, text, _ref12, _ref13, rows, _i;

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
                if (!(Array.isArray(events) && events.length === 0)) {
                  _context7.next = 4;
                  break;
                }

                throw new Error('Events are missing.');

              case 4:

                events = cloneDeep(flatten([events]));

                _context7.next = 7;
                return this.getDatabase();

              case 7:
                connection = _context7.sent;
                placeholders = [], values = [];
                i = 0;

              case 10:
                if (!(i < events.length)) {
                  _context7.next = 23;
                  break;
                }

                event = events[i];

                if (event.metadata) {
                  _context7.next = 14;
                  break;
                }

                throw new Error('Metadata are missing.');

              case 14:
                if (!(event.metadata.revision === undefined)) {
                  _context7.next = 16;
                  break;
                }

                throw new Error('Revision is missing.');

              case 16:
                if (!(event.metadata.revision < 1)) {
                  _context7.next = 18;
                  break;
                }

                throw new Error('Revision must not be less than 1.');

              case 18:

                placeholders.push('(UuidToBin(?), ?, ?, ?)');
                values.push(event.aggregate.id, event.metadata.revision, (0, _stringify2.default)(event), event.metadata.published);

              case 20:
                i++;
                _context7.next = 10;
                break;

              case 23:
                text = '\n      INSERT INTO ' + this.namespace + '_events\n        (aggregateId, revision, event, hasBeenPublished)\n      VALUES\n        ' + placeholders.join(',') + ';\n    ';
                _context7.prev = 24;
                _context7.next = 27;
                return connection.execute(text, values);

              case 27:
                _context7.next = 29;
                return connection.execute('SELECT LAST_INSERT_ID() AS position;');

              case 29:
                _ref12 = _context7.sent;
                _ref13 = (0, _slicedToArray3.default)(_ref12, 1);
                rows = _ref13[0];


                // We only get the ID of the first inserted row, but since it's all in a
                // single INSERT statement, the database guarantees that the positions are
                // sequential, so we easily calculate them by ourselves.
                for (_i = 0; _i < events.length; _i++) {
                  events[_i].metadata.position = Number(rows[0].position) + _i;
                }

                return _context7.abrupt('return', events);

              case 36:
                _context7.prev = 36;
                _context7.t0 = _context7['catch'](24);

                if (!(_context7.t0.code === 'ER_DUP_ENTRY' && _context7.t0.sqlMessage.endsWith('for key \'aggregateId\''))) {
                  _context7.next = 40;
                  break;
                }

                throw new Error('Aggregate id and revision already exist.');

              case 40:
                throw _context7.t0;

              case 41:
                _context7.prev = 41;

                connection.release();
                return _context7.finish(41);

              case 44:
              case 'end':
                return _context7.stop();
            }
          }
        }, _callee7, this, [[24, 36, 41, 44]]);
      }));

      function saveEvents(_x5) {
        return _ref11.apply(this, arguments);
      }

      return saveEvents;
    }()
  }, {
    key: 'markEventsAsPublished',
    value: function () {
      var _ref15 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee8(_ref14) {
        var aggregateId = _ref14.aggregateId,
            fromRevision = _ref14.fromRevision,
            toRevision = _ref14.toRevision;
        var connection;
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
                connection = _context8.sent;
                _context8.prev = 11;
                _context8.next = 14;
                return connection.execute('\n        UPDATE ' + this.namespace + '_events\n          SET hasBeenPublished = true\n          WHERE aggregateId = UuidToBin(?)\n            AND revision >= ?\n            AND revision <= ?\n      ', [aggregateId, fromRevision, toRevision]);

              case 14:
                _context8.prev = 14;
                _context8.next = 17;
                return connection.release();

              case 17:
                return _context8.finish(14);

              case 18:
              case 'end':
                return _context8.stop();
            }
          }
        }, _callee8, this, [[11,, 14, 18]]);
      }));

      function markEventsAsPublished(_x6) {
        return _ref15.apply(this, arguments);
      }

      return markEventsAsPublished;
    }()
  }, {
    key: 'getSnapshot',
    value: function () {
      var _ref16 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee9(aggregateId) {
        var connection, _ref17, _ref18, rows;

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
                connection = _context9.sent;
                _context9.prev = 5;
                _context9.next = 8;
                return connection.execute('\n        SELECT state, revision\n          FROM ' + this.namespace + '_snapshots\n          WHERE aggregateId = UuidToBin(?)\n          ORDER BY revision DESC\n          LIMIT 1\n      ', [aggregateId]);

              case 8:
                _ref17 = _context9.sent;
                _ref18 = (0, _slicedToArray3.default)(_ref17, 1);
                rows = _ref18[0];

                if (!(rows.length === 0)) {
                  _context9.next = 13;
                  break;
                }

                return _context9.abrupt('return');

              case 13:
                return _context9.abrupt('return', {
                  revision: rows[0].revision,
                  state: JSON.parse(rows[0].state)
                });

              case 14:
                _context9.prev = 14;
                _context9.next = 17;
                return connection.release();

              case 17:
                return _context9.finish(14);

              case 18:
              case 'end':
                return _context9.stop();
            }
          }
        }, _callee9, this, [[5,, 14, 18]]);
      }));

      function getSnapshot(_x7) {
        return _ref16.apply(this, arguments);
      }

      return getSnapshot;
    }()
  }, {
    key: 'saveSnapshot',
    value: function () {
      var _ref20 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee10(_ref19) {
        var aggregateId = _ref19.aggregateId,
            revision = _ref19.revision,
            state = _ref19.state;
        var connection;
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
                connection = _context10.sent;
                _context10.prev = 10;
                _context10.next = 13;
                return connection.execute('\n        INSERT IGNORE INTO ' + this.namespace + '_snapshots\n          (aggregateId, revision, state)\n          VALUES (UuidToBin(?), ?, ?);\n      ', [aggregateId, revision, (0, _stringify2.default)(state)]);

              case 13:
                _context10.prev = 13;
                _context10.next = 16;
                return connection.release();

              case 16:
                return _context10.finish(13);

              case 17:
              case 'end':
                return _context10.stop();
            }
          }
        }, _callee10, this, [[10,, 13, 17]]);
      }));

      function saveSnapshot(_x8) {
        return _ref20.apply(this, arguments);
      }

      return saveSnapshot;
    }()
  }, {
    key: 'getReplay',
    value: function () {
      var _ref21 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee11(options) {
        var fromPosition, toPosition, connection, passThrough, eventStream, onEnd, onError, onResult, unsubscribe;
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
                connection = _context11.sent;
                passThrough = new PassThrough({ objectMode: true });
                eventStream = connection.connection.execute('\n      SELECT event, position\n        FROM ' + this.namespace + '_events\n        WHERE position >= ?\n          AND position <= ?\n        ORDER BY position\n      ', [fromPosition, toPosition]);
                onEnd = void 0, onError = void 0, onResult = void 0;

                unsubscribe = function unsubscribe() {
                  connection.release();
                  eventStream.removeListener('end', onEnd);
                  eventStream.removeListener('error', onError);
                  eventStream.removeListener('result', onResult);
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

                onResult = function onResult(row) {
                  var event = Event.wrap(JSON.parse(row.event));

                  event.metadata.position = Number(row.position);
                  passThrough.write(event);
                };

                eventStream.on('end', onEnd);
                eventStream.on('error', onError);
                eventStream.on('result', onResult);

                return _context11.abrupt('return', passThrough);

              case 19:
              case 'end':
                return _context11.stop();
            }
          }
        }, _callee11, this);
      }));

      function getReplay(_x9) {
        return _ref21.apply(this, arguments);
      }

      return getReplay;
    }()
  }, {
    key: 'destroy',
    value: function () {
      var _ref22 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee12() {
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
        return _ref22.apply(this, arguments);
      }

      return destroy;
    }()
  }]);
  return Eventstore;
}(EventEmitter);

module.exports = Eventstore;