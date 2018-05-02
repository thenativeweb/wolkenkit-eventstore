'use strict';

var _stringify = require('babel-runtime/core-js/json/stringify');

var _stringify2 = _interopRequireDefault(_stringify);

var _promise = require('babel-runtime/core-js/promise');

var _promise2 = _interopRequireDefault(_promise);

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
    _require4 = require('tedious'),
    Request = _require4.Request,
    TYPES = _require4.TYPES;


var createPool = require('./createPool'),
    omitByDeep = require('../omitByDeep');

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
                return this.pool.acquire().promise;

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

        var _getParts, host, port, user, password, database, params, encrypt, connection, query;

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

                _getParts = new DsnParser(url).getParts(), host = _getParts.host, port = _getParts.port, user = _getParts.user, password = _getParts.password, database = _getParts.database, params = _getParts.params;
                encrypt = params.encrypt || false;


                this.pool = createPool({
                  host: host,
                  port: port,
                  user: user,
                  password: password,
                  database: database,
                  encrypt: encrypt,

                  onError: function onError() {
                    _this2.emit('error');
                  },

                  onDisconnect: function onDisconnect() {
                    _this2.emit('disconnect');
                  }
                });

                _context2.next = 10;
                return this.getDatabase();

              case 10:
                connection = _context2.sent;
                query = '\n      BEGIN TRANSACTION setupTables;\n\n      IF NOT EXISTS (SELECT [name] FROM sys.tables WHERE [name] = \'' + this.namespace + '_events\')\n        BEGIN\n          CREATE TABLE [' + this.namespace + '_events] (\n            [position] BIGINT IDENTITY(1,1),\n            [aggregateId] UNIQUEIDENTIFIER NOT NULL,\n            [revision] INT NOT NULL,\n            [event] NVARCHAR(4000) NOT NULL,\n            [hasBeenPublished] BIT NOT NULL,\n\n            CONSTRAINT [' + this.namespace + '_events_pk] PRIMARY KEY([position]),\n            CONSTRAINT [' + this.namespace + '_aggregateId_revision] UNIQUE ([aggregateId], [revision])\n          );\n        END\n\n      IF NOT EXISTS (SELECT [name] FROM sys.tables WHERE [name] = \'' + this.namespace + '_snapshots\')\n        BEGIN\n          CREATE TABLE [' + this.namespace + '_snapshots] (\n            [aggregateId] UNIQUEIDENTIFIER NOT NULL,\n            [revision] INT NOT NULL,\n            [state] NVARCHAR(4000) NOT NULL,\n\n            CONSTRAINT [' + this.namespace + '_snapshots_pk] PRIMARY KEY([aggregateId], [revision])\n          );\n        END\n\n      COMMIT TRANSACTION setupTables;\n    ';
                _context2.next = 14;
                return new _promise2.default(function (resolve, reject) {
                  var request = new Request(query, function (err) {
                    if (err) {
                      // When multiple clients initialize at the same time, e.g. during
                      // integration tests, SQL Server might throw an error. In this case
                      // we simply ignore it.
                      if (err.message.match(/There is already an object named.*_events/)) {
                        return resolve();
                      }

                      return reject(err);
                    }

                    resolve();
                  });

                  connection.execSql(request);
                });

              case 14:
                _context2.next = 16;
                return this.pool.release(connection);

              case 16:
              case 'end':
                return _context2.stop();
            }
          }
        }, _callee2, this);
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
        var _this3 = this;

        var database, result;
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
                database = _context3.sent;
                _context3.prev = 5;
                _context3.next = 8;
                return new _promise2.default(function (resolve, reject) {
                  var resultEvent = void 0;

                  var request = new Request('\n          SELECT TOP(1) [event], [position]\n            FROM ' + _this3.namespace + '_events\n            WHERE [aggregateId] = @aggregateId\n            ORDER BY [revision] DESC\n          ;\n        ', function (err) {
                    if (err) {
                      return reject(err);
                    }

                    resolve(resultEvent);
                  });

                  request.once('row', function (columns) {
                    resultEvent = Event.wrap(JSON.parse(columns[0].value));

                    resultEvent.metadata.position = Number(columns[1].value);
                  });

                  request.addParameter('aggregateId', TYPES.UniqueIdentifier, aggregateId);

                  database.execSql(request);
                });

              case 8:
                result = _context3.sent;

                if (result) {
                  _context3.next = 11;
                  break;
                }

                return _context3.abrupt('return');

              case 11:
                return _context3.abrupt('return', result);

              case 12:
                _context3.prev = 12;
                _context3.next = 15;
                return this.pool.release(database);

              case 15:
                return _context3.finish(12);

              case 16:
              case 'end':
                return _context3.stop();
            }
          }
        }, _callee3, this, [[5,, 12, 16]]);
      }));

      function getLastEvent(_x2) {
        return _ref4.apply(this, arguments);
      }

      return getLastEvent;
    }()
  }, {
    key: 'getEventStream',
    value: function () {
      var _ref5 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee4(aggregateId, options) {
        var _this4 = this;

        var fromRevision, toRevision, database, passThrough, onError, onRow, request, unsubscribe;
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
                database = _context4.sent;
                passThrough = new PassThrough({ objectMode: true });
                onError = void 0, onRow = void 0, request = void 0;

                unsubscribe = function unsubscribe() {
                  _this4.pool.release(database);
                  request.removeListener('row', onRow);
                  request.removeListener('error', onError);
                };

                onError = function onError(err) {
                  unsubscribe();
                  passThrough.emit('error', err);
                  passThrough.end();
                };

                onRow = function onRow(columns) {
                  var event = Event.wrap(JSON.parse(columns[0].value));

                  event.metadata.position = Number(columns[1].value);
                  event.metadata.published = columns[2].value;

                  passThrough.write(event);
                };

                request = new Request('\n      SELECT [event], [position], [hasBeenPublished]\n        FROM [' + this.namespace + '_events]\n        WHERE [aggregateId] = @aggregateId\n          AND [revision] >= @fromRevision\n          AND [revision] <= @toRevision\n        ORDER BY [revision]', function (err) {
                  unsubscribe();

                  if (err) {
                    passThrough.emit('error', err);
                  }

                  passThrough.end();
                });

                request.addParameter('aggregateId', TYPES.UniqueIdentifier, aggregateId);
                request.addParameter('fromRevision', TYPES.Int, fromRevision);
                request.addParameter('toRevision', TYPES.Int, toRevision);

                request.on('error', onError);
                request.on('row', onRow);

                database.execSql(request);

                return _context4.abrupt('return', passThrough);

              case 23:
              case 'end':
                return _context4.stop();
            }
          }
        }, _callee4, this);
      }));

      function getEventStream(_x3, _x4) {
        return _ref5.apply(this, arguments);
      }

      return getEventStream;
    }()
  }, {
    key: 'getUnpublishedEventStream',
    value: function () {
      var _ref6 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee5() {
        var _this5 = this;

        var database, passThrough, onError, onRow, request, unsubscribe;
        return _regenerator2.default.wrap(function _callee5$(_context5) {
          while (1) {
            switch (_context5.prev = _context5.next) {
              case 0:
                _context5.next = 2;
                return this.getDatabase();

              case 2:
                database = _context5.sent;
                passThrough = new PassThrough({ objectMode: true });
                onError = void 0, onRow = void 0, request = void 0;

                unsubscribe = function unsubscribe() {
                  _this5.pool.release(database);
                  request.removeListener('error', onError);
                  request.removeListener('row', onRow);
                };

                onError = function onError(err) {
                  unsubscribe();
                  passThrough.emit('error', err);
                  passThrough.end();
                };

                onRow = function onRow(columns) {
                  var event = Event.wrap(JSON.parse(columns[0].value));

                  event.metadata.position = Number(columns[1].value);
                  event.metadata.published = columns[2].value;

                  passThrough.write(event);
                };

                request = new Request('\n      SELECT [event], [position], [hasBeenPublished]\n        FROM [' + this.namespace + '_events]\n        WHERE [hasBeenPublished] = 0\n        ORDER BY [position]\n      ', function (err) {
                  unsubscribe();

                  if (err) {
                    passThrough.emit('error', err);
                  }

                  passThrough.end();
                });

                request.on('error', onError);
                request.on('row', onRow);

                database.execSql(request);

                return _context5.abrupt('return', passThrough);

              case 13:
              case 'end':
                return _context5.stop();
            }
          }
        }, _callee5, this);
      }));

      function getUnpublishedEventStream() {
        return _ref6.apply(this, arguments);
      }

      return getUnpublishedEventStream;
    }()
  }, {
    key: 'saveEvents',
    value: function () {
      var _ref8 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee6(_ref7) {
        var events = _ref7.events;
        var database, placeholders, values, resultCount, i, event, rowId, row, text, updatedEvents;
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
                database = _context6.sent;
                placeholders = [], values = [];
                resultCount = 0;


                for (i = 0; i < events.length; i++) {
                  event = events[i], rowId = i + 1;
                  row = [{ key: 'aggregateId' + rowId, value: event.aggregate.id, type: TYPES.UniqueIdentifier }, { key: 'revision' + rowId, value: event.metadata.revision, type: TYPES.Int }, { key: 'event' + rowId, value: (0, _stringify2.default)(event), type: TYPES.NVarChar, options: { length: 4000 } }, { key: 'hasBeenPublished' + rowId, value: event.metadata.published, type: TYPES.Bit }];


                  placeholders.push('(@' + row[0].key + ', @' + row[1].key + ', @' + row[2].key + ', @' + row[3].key + ')');

                  values.push.apply(values, row);
                }

                text = '\n      INSERT INTO [' + this.namespace + '_events] ([aggregateId], [revision], [event], [hasBeenPublished])\n        OUTPUT INSERTED.position\n      VALUES ' + placeholders.join(',') + ';\n    ';
                _context6.prev = 10;
                _context6.next = 13;
                return new _promise2.default(function (resolve, reject) {
                  var onRow = void 0;

                  var request = new Request(text, function (err) {
                    request.removeListener('row', onRow);

                    if (err) {
                      return reject(err);
                    }

                    resolve(events);
                  });

                  for (var _i = 0; _i < values.length; _i++) {
                    var value = values[_i];

                    request.addParameter(value.key, value.type, value.value, value.options);
                  }

                  onRow = function onRow(columns) {
                    events[resultCount].metadata.position = Number(columns[0].value);

                    resultCount += 1;
                  };

                  request.on('row', onRow);

                  database.execSql(request);
                });

              case 13:
                updatedEvents = _context6.sent;
                return _context6.abrupt('return', updatedEvents);

              case 17:
                _context6.prev = 17;
                _context6.t0 = _context6['catch'](10);

                if (!(_context6.t0.code === 'EREQUEST' && _context6.t0.number === 2627 && _context6.t0.message.includes('_aggregateId_revision'))) {
                  _context6.next = 21;
                  break;
                }

                throw new Error('Aggregate id and revision already exist.');

              case 21:
                throw _context6.t0;

              case 22:
                _context6.prev = 22;
                _context6.next = 25;
                return this.pool.release(database);

              case 25:
                return _context6.finish(22);

              case 26:
              case 'end':
                return _context6.stop();
            }
          }
        }, _callee6, this, [[10, 17, 22, 26]]);
      }));

      function saveEvents(_x5) {
        return _ref8.apply(this, arguments);
      }

      return saveEvents;
    }()
  }, {
    key: 'markEventsAsPublished',
    value: function () {
      var _ref10 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee7(_ref9) {
        var _this6 = this;

        var aggregateId = _ref9.aggregateId,
            fromRevision = _ref9.fromRevision,
            toRevision = _ref9.toRevision;
        var database;
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
                database = _context7.sent;
                _context7.prev = 11;
                _context7.next = 14;
                return new _promise2.default(function (resolve, reject) {
                  var request = new Request('\n          UPDATE [' + _this6.namespace + '_events]\n            SET [hasBeenPublished] = 1\n            WHERE [aggregateId] = @aggregateId\n              AND [revision] >= @fromRevision\n              AND [revision] <= @toRevision\n          ', function (err) {
                    if (err) {
                      return reject(err);
                    }

                    resolve();
                  });

                  request.addParameter('aggregateId', TYPES.UniqueIdentifier, aggregateId);
                  request.addParameter('fromRevision', TYPES.Int, fromRevision);
                  request.addParameter('toRevision', TYPES.Int, toRevision);

                  database.execSql(request);
                });

              case 14:
                _context7.prev = 14;
                _context7.next = 17;
                return this.pool.release(database);

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
        return _ref10.apply(this, arguments);
      }

      return markEventsAsPublished;
    }()
  }, {
    key: 'getSnapshot',
    value: function () {
      var _ref11 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee8(aggregateId) {
        var _this7 = this;

        var database, result;
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
                database = _context8.sent;
                _context8.prev = 5;
                _context8.next = 8;
                return new _promise2.default(function (resolve, reject) {
                  var resultRow = void 0;

                  var request = new Request('\n          SELECT TOP(1) [state], [revision]\n            FROM ' + _this7.namespace + '_snapshots\n            WHERE [aggregateId] = @aggregateId\n            ORDER BY [revision] DESC\n          ;', function (err) {
                    if (err) {
                      return reject(err);
                    }

                    resolve(resultRow);
                  });

                  request.once('row', function (columns) {
                    resultRow = {
                      state: JSON.parse(columns[0].value),
                      revision: Number(columns[1].value)
                    };
                  });

                  request.addParameter('aggregateId', TYPES.UniqueIdentifier, aggregateId);

                  database.execSql(request);
                });

              case 8:
                result = _context8.sent;

                if (result) {
                  _context8.next = 11;
                  break;
                }

                return _context8.abrupt('return');

              case 11:
                return _context8.abrupt('return', result);

              case 12:
                _context8.prev = 12;
                _context8.next = 15;
                return this.pool.release(database);

              case 15:
                return _context8.finish(12);

              case 16:
              case 'end':
                return _context8.stop();
            }
          }
        }, _callee8, this, [[5,, 12, 16]]);
      }));

      function getSnapshot(_x7) {
        return _ref11.apply(this, arguments);
      }

      return getSnapshot;
    }()
  }, {
    key: 'saveSnapshot',
    value: function () {
      var _ref13 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee9(_ref12) {
        var _this8 = this;

        var aggregateId = _ref12.aggregateId,
            revision = _ref12.revision,
            state = _ref12.state;
        var database;
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
                database = _context9.sent;
                _context9.prev = 10;
                _context9.next = 13;
                return new _promise2.default(function (resolve, reject) {
                  var request = new Request('\n          IF NOT EXISTS (SELECT TOP(1) * FROM ' + _this8.namespace + '_snapshots WHERE [aggregateId] = @aggregateId and [revision] = @revision)\n            BEGIN\n              INSERT INTO [' + _this8.namespace + '_snapshots] ([aggregateId], [revision], [state])\n              VALUES (@aggregateId, @revision, @state);\n            END\n          ', function (err) {
                    if (err) {
                      return reject(err);
                    }

                    resolve();
                  });

                  request.addParameter('aggregateId', TYPES.UniqueIdentifier, aggregateId);
                  request.addParameter('revision', TYPES.Int, revision);
                  request.addParameter('state', TYPES.NVarChar, (0, _stringify2.default)(state), { length: 4000 });

                  database.execSql(request);
                });

              case 13:
                _context9.prev = 13;
                _context9.next = 16;
                return this.pool.release(database);

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
        return _ref13.apply(this, arguments);
      }

      return saveSnapshot;
    }()
  }, {
    key: 'getReplay',
    value: function () {
      var _ref14 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee10(options) {
        var _this9 = this;

        var fromPosition, toPosition, database, passThrough, onError, onRow, request, unsubscribe;
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
                database = _context10.sent;
                passThrough = new PassThrough({ objectMode: true });
                onError = void 0, onRow = void 0, request = void 0;

                unsubscribe = function unsubscribe() {
                  _this9.pool.release(database);
                  request.removeListener('error', onError);
                  request.removeListener('row', onRow);
                };

                onError = function onError(err) {
                  unsubscribe();
                  passThrough.emit('error', err);
                  passThrough.end();
                };

                onRow = function onRow(columns) {
                  var event = Event.wrap(JSON.parse(columns[0].value));

                  event.metadata.position = Number(columns[1].value);

                  passThrough.write(event);
                };

                request = new Request('\n      SELECT [event], [position]\n        FROM [' + this.namespace + '_events]\n        WHERE [position] >= @fromPosition\n          AND [position] <= @toPosition\n        ORDER BY [position]', function (err) {
                  unsubscribe();

                  if (err) {
                    passThrough.emit('error', err);
                  }

                  passThrough.end();
                });

                request.addParameter('fromPosition', TYPES.BigInt, fromPosition);
                request.addParameter('toPosition', TYPES.BigInt, toPosition);

                request.on('error', onError);
                request.on('row', onRow);

                database.execSql(request);

                return _context10.abrupt('return', passThrough);

              case 20:
              case 'end':
                return _context10.stop();
            }
          }
        }, _callee10, this);
      }));

      function getReplay(_x9) {
        return _ref14.apply(this, arguments);
      }

      return getReplay;
    }()
  }, {
    key: 'destroy',
    value: function () {
      var _ref15 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee11() {
        return _regenerator2.default.wrap(function _callee11$(_context11) {
          while (1) {
            switch (_context11.prev = _context11.next) {
              case 0:
                if (!this.pool) {
                  _context11.next = 3;
                  break;
                }

                _context11.next = 3;
                return this.pool.destroy();

              case 3:
              case 'end':
                return _context11.stop();
            }
          }
        }, _callee11, this);
      }));

      function destroy() {
        return _ref15.apply(this, arguments);
      }

      return destroy;
    }()
  }]);
  return Eventstore;
}(EventEmitter);

module.exports = Eventstore;