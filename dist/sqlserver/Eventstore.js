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
    _require4 = require('tedious'),
    Request = _require4.Request,
    TYPES = _require4.TYPES,
    retry = require('async-retry');

var createPool = require('./createPool'),
    omitByDeep = require('../omitByDeep');

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
                          return _this.pool.acquire().promise;

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
      _regenerator.default.mark(function _callee3(_ref2) {
        var _this2 = this;

        var url, namespace, _getParts, host, port, user, password, database, params, encrypt, connection, query;

        return _regenerator.default.wrap(function _callee3$(_context3) {
          while (1) {
            switch (_context3.prev = _context3.next) {
              case 0:
                url = _ref2.url, namespace = _ref2.namespace;

                if (url) {
                  _context3.next = 3;
                  break;
                }

                throw new Error('Url is missing.');

              case 3:
                if (namespace) {
                  _context3.next = 5;
                  break;
                }

                throw new Error('Namespace is missing.');

              case 5:
                this.namespace = "store_".concat(limitAlphanumeric(namespace));
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
                _context3.next = 11;
                return this.getDatabase();

              case 11:
                connection = _context3.sent;
                query = "\n      BEGIN TRANSACTION setupTables;\n\n      IF NOT EXISTS (SELECT [name] FROM sys.tables WHERE [name] = '".concat(this.namespace, "_events')\n        BEGIN\n          CREATE TABLE [").concat(this.namespace, "_events] (\n            [position] BIGINT IDENTITY(1,1),\n            [aggregateId] UNIQUEIDENTIFIER NOT NULL,\n            [revision] INT NOT NULL,\n            [event] NVARCHAR(4000) NOT NULL,\n            [hasBeenPublished] BIT NOT NULL,\n\n            CONSTRAINT [").concat(this.namespace, "_events_pk] PRIMARY KEY([position]),\n            CONSTRAINT [").concat(this.namespace, "_aggregateId_revision] UNIQUE ([aggregateId], [revision])\n          );\n        END\n\n      IF NOT EXISTS (SELECT [name] FROM sys.tables WHERE [name] = '").concat(this.namespace, "_snapshots')\n        BEGIN\n          CREATE TABLE [").concat(this.namespace, "_snapshots] (\n            [aggregateId] UNIQUEIDENTIFIER NOT NULL,\n            [revision] INT NOT NULL,\n            [state] NVARCHAR(4000) NOT NULL,\n\n            CONSTRAINT [").concat(this.namespace, "_snapshots_pk] PRIMARY KEY([aggregateId], [revision])\n          );\n        END\n\n      COMMIT TRANSACTION setupTables;\n    ");
                _context3.next = 15;
                return new Promise(function (resolve, reject) {
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

              case 15:
                _context3.next = 17;
                return this.pool.release(connection);

              case 17:
              case "end":
                return _context3.stop();
            }
          }
        }, _callee3, this);
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
      _regenerator.default.mark(function _callee4(aggregateId) {
        var _this3 = this;

        var database, result;
        return _regenerator.default.wrap(function _callee4$(_context4) {
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
                return new Promise(function (resolve, reject) {
                  var resultEvent;
                  var request = new Request("\n          SELECT TOP(1) [event], [position]\n            FROM ".concat(_this3.namespace, "_events\n            WHERE [aggregateId] = @aggregateId\n            ORDER BY [revision] DESC\n          ;\n        "), function (err) {
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
                result = _context4.sent;

                if (result) {
                  _context4.next = 11;
                  break;
                }

                return _context4.abrupt("return");

              case 11:
                return _context4.abrupt("return", result);

              case 12:
                _context4.prev = 12;
                _context4.next = 15;
                return this.pool.release(database);

              case 15:
                return _context4.finish(12);

              case 16:
              case "end":
                return _context4.stop();
            }
          }
        }, _callee4, this, [[5,, 12, 16]]);
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
      _regenerator.default.mark(function _callee5(aggregateId, options) {
        var _this4 = this;

        var fromRevision, toRevision, database, passThrough, onError, onRow, request, unsubscribe;
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
                passThrough = new PassThrough({
                  objectMode: true
                });

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

                request = new Request("\n      SELECT [event], [position], [hasBeenPublished]\n        FROM [".concat(this.namespace, "_events]\n        WHERE [aggregateId] = @aggregateId\n          AND [revision] >= @fromRevision\n          AND [revision] <= @toRevision\n        ORDER BY [revision]"), function (err) {
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
                return _context5.abrupt("return", passThrough);

              case 22:
              case "end":
                return _context5.stop();
            }
          }
        }, _callee5, this);
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
      _regenerator.default.mark(function _callee6() {
        var _this5 = this;

        var database, passThrough, onError, onRow, request, unsubscribe;
        return _regenerator.default.wrap(function _callee6$(_context6) {
          while (1) {
            switch (_context6.prev = _context6.next) {
              case 0:
                _context6.next = 2;
                return this.getDatabase();

              case 2:
                database = _context6.sent;
                passThrough = new PassThrough({
                  objectMode: true
                });

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

                request = new Request("\n      SELECT [event], [position], [hasBeenPublished]\n        FROM [".concat(this.namespace, "_events]\n        WHERE [hasBeenPublished] = 0\n        ORDER BY [position]\n      "), function (err) {
                  unsubscribe();

                  if (err) {
                    passThrough.emit('error', err);
                  }

                  passThrough.end();
                });
                request.on('error', onError);
                request.on('row', onRow);
                database.execSql(request);
                return _context6.abrupt("return", passThrough);

              case 12:
              case "end":
                return _context6.stop();
            }
          }
        }, _callee6, this);
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
      _regenerator.default.mark(function _callee7(_ref3) {
        var events, placeholders, values, resultCount, i, event, rowId, row, database, text, updatedEvents;
        return _regenerator.default.wrap(function _callee7$(_context7) {
          while (1) {
            switch (_context7.prev = _context7.next) {
              case 0:
                events = _ref3.events;

                if (events) {
                  _context7.next = 3;
                  break;
                }

                throw new Error('Events are missing.');

              case 3:
                if (!(Array.isArray(events) && events.length === 0)) {
                  _context7.next = 5;
                  break;
                }

                throw new Error('Events are missing.');

              case 5:
                events = cloneDeep(flatten([events]));
                placeholders = [], values = [];
                resultCount = 0;
                i = 0;

              case 9:
                if (!(i < events.length)) {
                  _context7.next = 23;
                  break;
                }

                event = events[i], rowId = i + 1;

                if (event.metadata) {
                  _context7.next = 13;
                  break;
                }

                throw new Error('Metadata are missing.');

              case 13:
                if (!(event.metadata.revision === undefined)) {
                  _context7.next = 15;
                  break;
                }

                throw new Error('Revision is missing.');

              case 15:
                if (!(event.metadata.revision < 1)) {
                  _context7.next = 17;
                  break;
                }

                throw new Error('Revision must not be less than 1.');

              case 17:
                row = [{
                  key: "aggregateId".concat(rowId),
                  value: event.aggregate.id,
                  type: TYPES.UniqueIdentifier
                }, {
                  key: "revision".concat(rowId),
                  value: event.metadata.revision,
                  type: TYPES.Int
                }, {
                  key: "event".concat(rowId),
                  value: JSON.stringify(event),
                  type: TYPES.NVarChar,
                  options: {
                    length: 4000
                  }
                }, {
                  key: "hasBeenPublished".concat(rowId),
                  value: event.metadata.published,
                  type: TYPES.Bit
                }];
                placeholders.push("(@".concat(row[0].key, ", @").concat(row[1].key, ", @").concat(row[2].key, ", @").concat(row[3].key, ")"));
                values.push.apply(values, row);

              case 20:
                i++;
                _context7.next = 9;
                break;

              case 23:
                _context7.next = 25;
                return this.getDatabase();

              case 25:
                database = _context7.sent;
                text = "\n      INSERT INTO [".concat(this.namespace, "_events] ([aggregateId], [revision], [event], [hasBeenPublished])\n        OUTPUT INSERTED.position\n      VALUES ").concat(placeholders.join(','), ";\n    ");
                _context7.prev = 27;
                _context7.next = 30;
                return new Promise(function (resolve, reject) {
                  var onRow;
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

              case 30:
                updatedEvents = _context7.sent;
                return _context7.abrupt("return", updatedEvents);

              case 34:
                _context7.prev = 34;
                _context7.t0 = _context7["catch"](27);

                if (!(_context7.t0.code === 'EREQUEST' && _context7.t0.number === 2627 && _context7.t0.message.includes('_aggregateId_revision'))) {
                  _context7.next = 38;
                  break;
                }

                throw new Error('Aggregate id and revision already exist.');

              case 38:
                throw _context7.t0;

              case 39:
                _context7.prev = 39;
                _context7.next = 42;
                return this.pool.release(database);

              case 42:
                return _context7.finish(39);

              case 43:
              case "end":
                return _context7.stop();
            }
          }
        }, _callee7, this, [[27, 34, 39, 43]]);
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
      _regenerator.default.mark(function _callee8(_ref4) {
        var _this6 = this;

        var aggregateId, fromRevision, toRevision, database;
        return _regenerator.default.wrap(function _callee8$(_context8) {
          while (1) {
            switch (_context8.prev = _context8.next) {
              case 0:
                aggregateId = _ref4.aggregateId, fromRevision = _ref4.fromRevision, toRevision = _ref4.toRevision;

                if (aggregateId) {
                  _context8.next = 3;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 3:
                if (fromRevision) {
                  _context8.next = 5;
                  break;
                }

                throw new Error('From revision is missing.');

              case 5:
                if (toRevision) {
                  _context8.next = 7;
                  break;
                }

                throw new Error('To revision is missing.');

              case 7:
                if (!(fromRevision > toRevision)) {
                  _context8.next = 9;
                  break;
                }

                throw new Error('From revision is greater than to revision.');

              case 9:
                _context8.next = 11;
                return this.getDatabase();

              case 11:
                database = _context8.sent;
                _context8.prev = 12;
                _context8.next = 15;
                return new Promise(function (resolve, reject) {
                  var request = new Request("\n          UPDATE [".concat(_this6.namespace, "_events]\n            SET [hasBeenPublished] = 1\n            WHERE [aggregateId] = @aggregateId\n              AND [revision] >= @fromRevision\n              AND [revision] <= @toRevision\n          "), function (err) {
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

              case 15:
                _context8.prev = 15;
                _context8.next = 18;
                return this.pool.release(database);

              case 18:
                return _context8.finish(15);

              case 19:
              case "end":
                return _context8.stop();
            }
          }
        }, _callee8, this, [[12,, 15, 19]]);
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
      _regenerator.default.mark(function _callee9(aggregateId) {
        var _this7 = this;

        var database, result;
        return _regenerator.default.wrap(function _callee9$(_context9) {
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
                return new Promise(function (resolve, reject) {
                  var resultRow;
                  var request = new Request("\n          SELECT TOP(1) [state], [revision]\n            FROM ".concat(_this7.namespace, "_snapshots\n            WHERE [aggregateId] = @aggregateId\n            ORDER BY [revision] DESC\n          ;"), function (err) {
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
                result = _context9.sent;

                if (result) {
                  _context9.next = 11;
                  break;
                }

                return _context9.abrupt("return");

              case 11:
                return _context9.abrupt("return", result);

              case 12:
                _context9.prev = 12;
                _context9.next = 15;
                return this.pool.release(database);

              case 15:
                return _context9.finish(12);

              case 16:
              case "end":
                return _context9.stop();
            }
          }
        }, _callee9, this, [[5,, 12, 16]]);
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
      _regenerator.default.mark(function _callee10(_ref5) {
        var _this8 = this;

        var aggregateId, revision, state, database;
        return _regenerator.default.wrap(function _callee10$(_context10) {
          while (1) {
            switch (_context10.prev = _context10.next) {
              case 0:
                aggregateId = _ref5.aggregateId, revision = _ref5.revision, state = _ref5.state;

                if (aggregateId) {
                  _context10.next = 3;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 3:
                if (revision) {
                  _context10.next = 5;
                  break;
                }

                throw new Error('Revision is missing.');

              case 5:
                if (state) {
                  _context10.next = 7;
                  break;
                }

                throw new Error('State is missing.');

              case 7:
                state = omitByDeep(state, function (value) {
                  return value === undefined;
                });
                _context10.next = 10;
                return this.getDatabase();

              case 10:
                database = _context10.sent;
                _context10.prev = 11;
                _context10.next = 14;
                return new Promise(function (resolve, reject) {
                  var request = new Request("\n          IF NOT EXISTS (SELECT TOP(1) * FROM ".concat(_this8.namespace, "_snapshots WHERE [aggregateId] = @aggregateId and [revision] = @revision)\n            BEGIN\n              INSERT INTO [").concat(_this8.namespace, "_snapshots] ([aggregateId], [revision], [state])\n              VALUES (@aggregateId, @revision, @state);\n            END\n          "), function (err) {
                    if (err) {
                      return reject(err);
                    }

                    resolve();
                  });
                  request.addParameter('aggregateId', TYPES.UniqueIdentifier, aggregateId);
                  request.addParameter('revision', TYPES.Int, revision);
                  request.addParameter('state', TYPES.NVarChar, JSON.stringify(state), {
                    length: 4000
                  });
                  database.execSql(request);
                });

              case 14:
                _context10.prev = 14;
                _context10.next = 17;
                return this.pool.release(database);

              case 17:
                return _context10.finish(14);

              case 18:
              case "end":
                return _context10.stop();
            }
          }
        }, _callee10, this, [[11,, 14, 18]]);
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
      _regenerator.default.mark(function _callee11(options) {
        var _this9 = this;

        var fromPosition, toPosition, database, passThrough, onError, onRow, request, unsubscribe;
        return _regenerator.default.wrap(function _callee11$(_context11) {
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
                passThrough = new PassThrough({
                  objectMode: true
                });

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

                request = new Request("\n      SELECT [event], [position]\n        FROM [".concat(this.namespace, "_events]\n        WHERE [position] >= @fromPosition\n          AND [position] <= @toPosition\n        ORDER BY [position]"), function (err) {
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
                return _context11.abrupt("return", passThrough);

              case 19:
              case "end":
                return _context11.stop();
            }
          }
        }, _callee11, this);
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
      _regenerator.default.mark(function _callee12() {
        return _regenerator.default.wrap(function _callee12$(_context12) {
          while (1) {
            switch (_context12.prev = _context12.next) {
              case 0:
                if (!this.pool) {
                  _context12.next = 3;
                  break;
                }

                _context12.next = 3;
                return this.pool.destroy();

              case 3:
              case "end":
                return _context12.stop();
            }
          }
        }, _callee12, this);
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