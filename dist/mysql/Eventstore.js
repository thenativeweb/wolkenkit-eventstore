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
    mysql = require('mysql2/promise');


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
                return this.pool.getConnection();

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
      var _ref3 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee2(_ref2) {
        var _this2 = this;

        var url = _ref2.url,
            namespace = _ref2.namespace;

        var _getParts, host, port, user, password, database, connection, createUuidToBinFunction, createUuidFromBinFunction, query;

        return _regenerator2.default.wrap(function _callee2$(_context2) {
          while (1) {
            switch (_context2.prev = _context2.next) {
              case 0:
                if (url) {
                  _context2.next = 2;
                  break;
                }

                throw new Error('Url is missing.');

              case 2:
                if (namespace) {
                  _context2.next = 4;
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
                    _this2.emit('disconnect');
                  });
                  connection.on('end', function () {
                    _this2.emit('disconnect');
                  });
                });

                _context2.next = 10;
                return this.getDatabase();

              case 10:
                connection = _context2.sent;
                createUuidToBinFunction = '\n      CREATE FUNCTION UuidToBin(_uuid BINARY(36))\n        RETURNS BINARY(16)\n        RETURN UNHEX(CONCAT(\n          SUBSTR(_uuid, 15, 4),\n          SUBSTR(_uuid, 10, 4),\n          SUBSTR(_uuid, 1, 8),\n          SUBSTR(_uuid, 20, 4),\n          SUBSTR(_uuid, 25)\n        ));\n    ';
                _context2.prev = 12;
                _context2.next = 15;
                return connection.query(createUuidToBinFunction);

              case 15:
                _context2.next = 21;
                break;

              case 17:
                _context2.prev = 17;
                _context2.t0 = _context2['catch'](12);

                if (_context2.t0.message.includes('FUNCTION UuidToBin already exists')) {
                  _context2.next = 21;
                  break;
                }

                throw _context2.t0;

              case 21:
                createUuidFromBinFunction = '\n      CREATE FUNCTION UuidFromBin(_bin BINARY(16))\n        RETURNS BINARY(36)\n        RETURN LCASE(CONCAT_WS(\'-\',\n          HEX(SUBSTR(_bin,  5, 4)),\n          HEX(SUBSTR(_bin,  3, 2)),\n          HEX(SUBSTR(_bin,  1, 2)),\n          HEX(SUBSTR(_bin,  9, 2)),\n          HEX(SUBSTR(_bin, 11))\n        ));\n    ';
                _context2.prev = 22;
                _context2.next = 25;
                return connection.query(createUuidFromBinFunction);

              case 25:
                _context2.next = 31;
                break;

              case 27:
                _context2.prev = 27;
                _context2.t1 = _context2['catch'](22);

                if (_context2.t1.message.includes('FUNCTION UuidFromBin already exists')) {
                  _context2.next = 31;
                  break;
                }

                throw _context2.t1;

              case 31:
                query = '\n      CREATE TABLE IF NOT EXISTS ' + this.namespace + '_events (\n        position SERIAL,\n        aggregateId BINARY(16) NOT NULL,\n        revision INT NOT NULL,\n        event JSON NOT NULL,\n        hasBeenPublished BOOLEAN NOT NULL,\n\n        PRIMARY KEY(position),\n        UNIQUE (aggregateId, revision)\n      ) ENGINE = InnoDB;\n\n      CREATE TABLE IF NOT EXISTS ' + this.namespace + '_snapshots (\n        aggregateId BINARY(16) NOT NULL,\n        revision INT NOT NULL,\n        state JSON NOT NULL,\n\n        PRIMARY KEY(aggregateId, revision)\n      ) ENGINE = InnoDB;\n    ';
                _context2.next = 34;
                return connection.query(query);

              case 34:
                _context2.next = 36;
                return connection.release();

              case 36:
              case 'end':
                return _context2.stop();
            }
          }
        }, _callee2, this, [[12, 17], [22, 27]]);
      }));

      function initialize(_x) {
        return _ref3.apply(this, arguments);
      }

      return initialize;
    }()
  }, {
    key: 'getLastEvent',
    value: function () {
      var _ref4 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee3(aggregateId) {
        var connection, _ref5, _ref6, rows, event;

        return _regenerator2.default.wrap(function _callee3$(_context3) {
          while (1) {
            switch (_context3.prev = _context3.next) {
              case 0:
                if (aggregateId) {
                  _context3.next = 2;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 2:
                _context3.next = 4;
                return this.getDatabase();

              case 4:
                connection = _context3.sent;
                _context3.prev = 5;
                _context3.next = 8;
                return connection.execute('\n        SELECT event, position\n          FROM ' + this.namespace + '_events\n            WHERE aggregateId = UuidToBin(?)\n          ORDER BY revision DESC\n          LIMIT 1\n        ', [aggregateId]);

              case 8:
                _ref5 = _context3.sent;
                _ref6 = (0, _slicedToArray3.default)(_ref5, 1);
                rows = _ref6[0];

                if (!(rows.length === 0)) {
                  _context3.next = 13;
                  break;
                }

                return _context3.abrupt('return');

              case 13:
                event = Event.wrap(rows[0].event);


                event.metadata.position = Number(rows[0].position);

                return _context3.abrupt('return', event);

              case 16:
                _context3.prev = 16;
                _context3.next = 19;
                return connection.release();

              case 19:
                return _context3.finish(16);

              case 20:
              case 'end':
                return _context3.stop();
            }
          }
        }, _callee3, this, [[5,, 16, 20]]);
      }));

      function getLastEvent(_x2) {
        return _ref4.apply(this, arguments);
      }

      return getLastEvent;
    }()
  }, {
    key: 'getEventStream',
    value: function () {
      var _ref7 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee4(aggregateId, options) {
        var fromRevision, toRevision, connection, passThrough, eventStream, onEnd, onError, onResult, unsubscribe;
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

                options = options || {};

                fromRevision = options.fromRevision || 1;
                toRevision = options.toRevision || Math.pow(2, 31) - 1;

                if (!(fromRevision > toRevision)) {
                  _context4.next = 7;
                  break;
                }

                throw new Error('From revision is greater than to revision.');

              case 7:
                _context4.next = 9;
                return this.getDatabase();

              case 9:
                connection = _context4.sent;
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
                  var event = Event.wrap(row.event);

                  event.metadata.position = Number(row.position);
                  event.metadata.published = Boolean(row.hasBeenPublished);

                  passThrough.write(event);
                };

                eventStream.on('end', onEnd);
                eventStream.on('error', onError);
                eventStream.on('result', onResult);

                return _context4.abrupt('return', passThrough);

              case 21:
              case 'end':
                return _context4.stop();
            }
          }
        }, _callee4, this);
      }));

      function getEventStream(_x3, _x4) {
        return _ref7.apply(this, arguments);
      }

      return getEventStream;
    }()
  }, {
    key: 'getUnpublishedEventStream',
    value: function () {
      var _ref8 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee5() {
        var connection, passThrough, eventStream, onEnd, onError, onResult, unsubscribe;
        return _regenerator2.default.wrap(function _callee5$(_context5) {
          while (1) {
            switch (_context5.prev = _context5.next) {
              case 0:
                _context5.next = 2;
                return this.getDatabase();

              case 2:
                connection = _context5.sent;
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
                  var event = Event.wrap(row.event);

                  event.metadata.position = Number(row.position);
                  event.metadata.published = Boolean(row.hasBeenPublished);
                  passThrough.write(event);
                };

                eventStream.on('end', onEnd);
                eventStream.on('error', onError);
                eventStream.on('result', onResult);

                return _context5.abrupt('return', passThrough);

              case 14:
              case 'end':
                return _context5.stop();
            }
          }
        }, _callee5, this);
      }));

      function getUnpublishedEventStream() {
        return _ref8.apply(this, arguments);
      }

      return getUnpublishedEventStream;
    }()
  }, {
    key: 'saveEvents',
    value: function () {
      var _ref10 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee6(_ref9) {
        var events = _ref9.events;

        var connection, placeholders, values, i, event, text, _ref11, _ref12, rows, _i;

        return _regenerator2.default.wrap(function _callee6$(_context6) {
          while (1) {
            switch (_context6.prev = _context6.next) {
              case 0:
                if (events) {
                  _context6.next = 2;
                  break;
                }

                throw new Error('Events are missing.');

              case 2:

                events = cloneDeep(flatten([events]));

                _context6.next = 5;
                return this.getDatabase();

              case 5:
                connection = _context6.sent;
                placeholders = [], values = [];


                for (i = 0; i < events.length; i++) {
                  event = events[i];


                  placeholders.push('(UuidToBin(?), ?, ?, ?)');
                  values.push(event.aggregate.id, event.metadata.revision, (0, _stringify2.default)(event), event.metadata.published);
                }

                text = '\n      INSERT INTO ' + this.namespace + '_events\n        (aggregateId, revision, event, hasBeenPublished)\n      VALUES\n        ' + placeholders.join(',') + ';\n    ';
                _context6.prev = 9;
                _context6.next = 12;
                return connection.execute(text, values);

              case 12:
                _context6.next = 14;
                return connection.execute('SELECT LAST_INSERT_ID() AS position;');

              case 14:
                _ref11 = _context6.sent;
                _ref12 = (0, _slicedToArray3.default)(_ref11, 1);
                rows = _ref12[0];


                // We only get the ID of the first inserted row, but since it's all in a
                // single INSERT statement, the database guarantees that the positions are
                // sequential, so we easily calculate them by ourselves.
                for (_i = 0; _i < events.length; _i++) {
                  events[_i].metadata.position = Number(rows[0].position) + _i;
                }

                return _context6.abrupt('return', events);

              case 21:
                _context6.prev = 21;
                _context6.t0 = _context6['catch'](9);

                if (!(_context6.t0.code === 'ER_DUP_ENTRY' && _context6.t0.sqlMessage.endsWith('for key \'aggregateId\''))) {
                  _context6.next = 25;
                  break;
                }

                throw new Error('Aggregate id and revision already exist.');

              case 25:
                throw _context6.t0;

              case 26:
                _context6.prev = 26;

                connection.release();
                return _context6.finish(26);

              case 29:
              case 'end':
                return _context6.stop();
            }
          }
        }, _callee6, this, [[9, 21, 26, 29]]);
      }));

      function saveEvents(_x5) {
        return _ref10.apply(this, arguments);
      }

      return saveEvents;
    }()
  }, {
    key: 'markEventsAsPublished',
    value: function () {
      var _ref14 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee7(_ref13) {
        var aggregateId = _ref13.aggregateId,
            fromRevision = _ref13.fromRevision,
            toRevision = _ref13.toRevision;
        var connection;
        return _regenerator2.default.wrap(function _callee7$(_context7) {
          while (1) {
            switch (_context7.prev = _context7.next) {
              case 0:
                if (aggregateId) {
                  _context7.next = 2;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 2:
                if (fromRevision) {
                  _context7.next = 4;
                  break;
                }

                throw new Error('From revision is missing.');

              case 4:
                if (toRevision) {
                  _context7.next = 6;
                  break;
                }

                throw new Error('To revision is missing.');

              case 6:
                if (!(fromRevision > toRevision)) {
                  _context7.next = 8;
                  break;
                }

                throw new Error('From revision is greater than to revision.');

              case 8:
                _context7.next = 10;
                return this.getDatabase();

              case 10:
                connection = _context7.sent;
                _context7.prev = 11;
                _context7.next = 14;
                return connection.execute('\n        UPDATE ' + this.namespace + '_events\n          SET hasBeenPublished = true\n          WHERE aggregateId = UuidToBin(?)\n            AND revision >= ?\n            AND revision <= ?\n      ', [aggregateId, fromRevision, toRevision]);

              case 14:
                _context7.prev = 14;
                _context7.next = 17;
                return connection.release();

              case 17:
                return _context7.finish(14);

              case 18:
              case 'end':
                return _context7.stop();
            }
          }
        }, _callee7, this, [[11,, 14, 18]]);
      }));

      function markEventsAsPublished(_x6) {
        return _ref14.apply(this, arguments);
      }

      return markEventsAsPublished;
    }()
  }, {
    key: 'getSnapshot',
    value: function () {
      var _ref15 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee8(aggregateId) {
        var connection, _ref16, _ref17, rows;

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
                _context8.next = 4;
                return this.getDatabase();

              case 4:
                connection = _context8.sent;
                _context8.prev = 5;
                _context8.next = 8;
                return connection.execute('\n        SELECT state, revision\n          FROM ' + this.namespace + '_snapshots\n          WHERE aggregateId = UuidToBin(?)\n          ORDER BY revision DESC\n          LIMIT 1\n      ', [aggregateId]);

              case 8:
                _ref16 = _context8.sent;
                _ref17 = (0, _slicedToArray3.default)(_ref16, 1);
                rows = _ref17[0];

                if (!(rows.length === 0)) {
                  _context8.next = 13;
                  break;
                }

                return _context8.abrupt('return');

              case 13:
                return _context8.abrupt('return', {
                  revision: rows[0].revision,
                  state: rows[0].state
                });

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
        }, _callee8, this, [[5,, 14, 18]]);
      }));

      function getSnapshot(_x7) {
        return _ref15.apply(this, arguments);
      }

      return getSnapshot;
    }()
  }, {
    key: 'saveSnapshot',
    value: function () {
      var _ref19 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee9(_ref18) {
        var aggregateId = _ref18.aggregateId,
            revision = _ref18.revision,
            state = _ref18.state;
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
                if (revision) {
                  _context9.next = 4;
                  break;
                }

                throw new Error('Revision is missing.');

              case 4:
                if (state) {
                  _context9.next = 6;
                  break;
                }

                throw new Error('State is missing.');

              case 6:

                state = omitByDeep(state, function (value) {
                  return value === undefined;
                });

                _context9.next = 9;
                return this.getDatabase();

              case 9:
                connection = _context9.sent;
                _context9.prev = 10;
                _context9.next = 13;
                return connection.execute('\n        INSERT IGNORE INTO ' + this.namespace + '_snapshots\n          (aggregateId, revision, state)\n          VALUES (UuidToBin(?), ?, ?);\n      ', [aggregateId, revision, (0, _stringify2.default)(state)]);

              case 13:
                _context9.prev = 13;
                _context9.next = 16;
                return connection.release();

              case 16:
                return _context9.finish(13);

              case 17:
              case 'end':
                return _context9.stop();
            }
          }
        }, _callee9, this, [[10,, 13, 17]]);
      }));

      function saveSnapshot(_x8) {
        return _ref19.apply(this, arguments);
      }

      return saveSnapshot;
    }()
  }, {
    key: 'getReplay',
    value: function () {
      var _ref20 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee10(options) {
        var fromPosition, toPosition, connection, passThrough, eventStream, onEnd, onError, onResult, unsubscribe;
        return _regenerator2.default.wrap(function _callee10$(_context10) {
          while (1) {
            switch (_context10.prev = _context10.next) {
              case 0:
                options = options || {};

                fromPosition = options.fromPosition || 1;
                toPosition = options.toPosition || Math.pow(2, 31) - 1;

                if (!(fromPosition > toPosition)) {
                  _context10.next = 5;
                  break;
                }

                throw new Error('From position is greater than to position.');

              case 5:
                _context10.next = 7;
                return this.getDatabase();

              case 7:
                connection = _context10.sent;
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
                  var event = Event.wrap(row.event);

                  event.metadata.position = Number(row.position);
                  passThrough.write(event);
                };

                eventStream.on('end', onEnd);
                eventStream.on('error', onError);
                eventStream.on('result', onResult);

                return _context10.abrupt('return', passThrough);

              case 19:
              case 'end':
                return _context10.stop();
            }
          }
        }, _callee10, this);
      }));

      function getReplay(_x9) {
        return _ref20.apply(this, arguments);
      }

      return getReplay;
    }()
  }, {
    key: 'destroy',
    value: function () {
      var _ref21 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee11() {
        return _regenerator2.default.wrap(function _callee11$(_context11) {
          while (1) {
            switch (_context11.prev = _context11.next) {
              case 0:
                if (!this.pool) {
                  _context11.next = 3;
                  break;
                }

                _context11.next = 3;
                return this.pool.end();

              case 3:
              case 'end':
                return _context11.stop();
            }
          }
        }, _callee11, this);
      }));

      function destroy() {
        return _ref21.apply(this, arguments);
      }

      return destroy;
    }()
  }]);
  return Eventstore;
}(EventEmitter);

module.exports = Eventstore;