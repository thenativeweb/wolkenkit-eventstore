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
    _require2 = require('url'),
    parse = _require2.parse,
    _require3 = require('stream'),
    PassThrough = _require3.PassThrough;

var cloneDeep = require('lodash/cloneDeep'),
    _require4 = require('commands-events'),
    Event = _require4.Event,
    flatten = require('lodash/flatten'),
    limitAlphanumeric = require('limit-alphanumeric'),
    _require5 = require('mongodb'),
    MongoClient = _require5.MongoClient,
    retry = require('async-retry');

var omitByDeep = require('../omitByDeep');

var Eventstore =
/*#__PURE__*/
function (_EventEmitter) {
  (0, _inherits2.default)(Eventstore, _EventEmitter);

  function Eventstore() {
    var _this;

    (0, _classCallCheck2.default)(this, Eventstore);
    _this = (0, _possibleConstructorReturn2.default)(this, (0, _getPrototypeOf2.default)(Eventstore).call(this));
    _this.client = undefined;
    _this.db = undefined;
    _this.collections = {};
    return _this;
  }

  (0, _createClass2.default)(Eventstore, [{
    key: "initialize",
    value: function () {
      var _initialize = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee2(_ref) {
        var _this2 = this;

        var url, namespace, databaseName;
        return _regenerator.default.wrap(function _callee2$(_context2) {
          while (1) {
            switch (_context2.prev = _context2.next) {
              case 0:
                url = _ref.url, namespace = _ref.namespace;

                if (url) {
                  _context2.next = 3;
                  break;
                }

                throw new Error('Url is missing.');

              case 3:
                if (namespace) {
                  _context2.next = 5;
                  break;
                }

                throw new Error('Namespace is missing.');

              case 5:
                this.namespace = "store_".concat(limitAlphanumeric(namespace));
                /* eslint-disable id-length */

                _context2.next = 8;
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
                          return MongoClient.connect(url, {
                            w: 1,
                            useNewUrlParser: true
                          });

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

              case 8:
                this.client = _context2.sent;

                /* eslint-enable id-length */
                databaseName = parse(url).pathname.substring(1);
                this.db = this.client.db(databaseName);
                this.db.on('close', function () {
                  _this2.emit('disconnect');
                });
                this.collections.events = this.db.collection("".concat(namespace, "_events"));
                this.collections.snapshots = this.db.collection("".concat(namespace, "_snapshots"));
                this.collections.counters = this.db.collection("".concat(namespace, "_counters"));
                _context2.next = 17;
                return this.collections.events.ensureIndex({
                  'aggregate.id': 1
                }, {
                  name: "".concat(this.namespace, "_aggregateId")
                });

              case 17:
                _context2.next = 19;
                return this.collections.events.ensureIndex({
                  'aggregate.id': 1,
                  'metadata.revision': 1
                }, {
                  unique: true,
                  name: "".concat(this.namespace, "_aggregateId_revision")
                });

              case 19:
                _context2.next = 21;
                return this.collections.events.ensureIndex({
                  'metadata.position': 1
                }, {
                  unique: true,
                  name: "".concat(this.namespace, "_position")
                });

              case 21:
                _context2.next = 23;
                return this.collections.snapshots.ensureIndex({
                  aggregateId: 1
                }, {
                  unique: true
                });

              case 23:
                _context2.prev = 23;
                _context2.next = 26;
                return this.collections.counters.insertOne({
                  _id: 'events',
                  seq: 0
                });

              case 26:
                _context2.next = 33;
                break;

              case 28:
                _context2.prev = 28;
                _context2.t0 = _context2["catch"](23);

                if (!(_context2.t0.code === 11000 && _context2.t0.message.includes('_counters index: _id_ dup key'))) {
                  _context2.next = 32;
                  break;
                }

                return _context2.abrupt("return");

              case 32:
                throw _context2.t0;

              case 33:
              case "end":
                return _context2.stop();
            }
          }
        }, _callee2, this, [[23, 28]]);
      }));

      function initialize(_x) {
        return _initialize.apply(this, arguments);
      }

      return initialize;
    }()
  }, {
    key: "getNextSequence",
    value: function () {
      var _getNextSequence = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee3(name) {
        var counter;
        return _regenerator.default.wrap(function _callee3$(_context3) {
          while (1) {
            switch (_context3.prev = _context3.next) {
              case 0:
                _context3.next = 2;
                return this.collections.counters.findOneAndUpdate({
                  _id: name
                }, {
                  $inc: {
                    seq: 1
                  }
                }, {
                  returnOriginal: false
                });

              case 2:
                counter = _context3.sent;
                return _context3.abrupt("return", counter.value.seq);

              case 4:
              case "end":
                return _context3.stop();
            }
          }
        }, _callee3, this);
      }));

      function getNextSequence(_x2) {
        return _getNextSequence.apply(this, arguments);
      }

      return getNextSequence;
    }()
  }, {
    key: "getLastEvent",
    value: function () {
      var _getLastEvent = (0, _asyncToGenerator2.default)(
      /*#__PURE__*/
      _regenerator.default.mark(function _callee4(aggregateId) {
        var events;
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
                return this.collections.events.find({
                  'aggregate.id': aggregateId
                }, {
                  fields: {
                    _id: 0
                  },
                  sort: {
                    'metadata.revision': -1
                  },
                  limit: 1
                }).toArray();

              case 4:
                events = _context4.sent;

                if (!(events.length === 0)) {
                  _context4.next = 7;
                  break;
                }

                return _context4.abrupt("return");

              case 7:
                return _context4.abrupt("return", Event.wrap(events[0]));

              case 8:
              case "end":
                return _context4.stop();
            }
          }
        }, _callee4, this);
      }));

      function getLastEvent(_x3) {
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
        var fromRevision, toRevision, passThrough, eventStream, onData, onEnd, onError, unsubscribe;
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
                passThrough = new PassThrough({
                  objectMode: true
                });
                eventStream = this.collections.events.find({
                  $and: [{
                    'aggregate.id': aggregateId
                  }, {
                    'metadata.revision': {
                      $gte: fromRevision
                    }
                  }, {
                    'metadata.revision': {
                      $lte: toRevision
                    }
                  }]
                }, {
                  fields: {
                    _id: 0
                  },
                  sort: 'metadata.revision'
                }).stream();

                unsubscribe = function unsubscribe() {
                  eventStream.removeListener('data', onData);
                  eventStream.removeListener('end', onEnd);
                  eventStream.removeListener('error', onError);
                };

                onData = function onData(data) {
                  passThrough.write(Event.wrap(data));
                };

                onEnd = function onEnd() {
                  unsubscribe();
                  passThrough.end(); // In the PostgreSQL eventstore, we call eventStream.end() here. In MongoDB,
                  // this function apparently is not implemented. This note is just for
                  // informational purposes to ensure that you are aware that the two
                  // implementations differ here.
                };

                onError = function onError(err) {
                  unsubscribe();
                  passThrough.emit('error', err);
                  passThrough.end(); // In the PostgreSQL eventstore, we call eventStream.end() here. In MongoDB,
                  // this function apparently is not implemented. This note is just for
                  // informational purposes to ensure that you are aware that the two
                  // implementations differ here.
                };

                eventStream.on('data', onData);
                eventStream.on('end', onEnd);
                eventStream.on('error', onError);
                return _context5.abrupt("return", passThrough);

              case 17:
              case "end":
                return _context5.stop();
            }
          }
        }, _callee5, this);
      }));

      function getEventStream(_x4, _x5) {
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
        var passThrough, eventStream, onData, onEnd, onError, unsubscribe;
        return _regenerator.default.wrap(function _callee6$(_context6) {
          while (1) {
            switch (_context6.prev = _context6.next) {
              case 0:
                passThrough = new PassThrough({
                  objectMode: true
                });
                eventStream = this.collections.events.find({
                  'metadata.published': false
                }, {
                  fields: {
                    _id: 0
                  },
                  sort: 'metadata.position'
                }).stream();

                unsubscribe = function unsubscribe() {
                  eventStream.removeListener('data', onData);
                  eventStream.removeListener('end', onEnd);
                  eventStream.removeListener('error', onError);
                };

                onData = function onData(data) {
                  passThrough.write(Event.wrap(data));
                };

                onEnd = function onEnd() {
                  unsubscribe();
                  passThrough.end(); // In the PostgreSQL eventstore, we call eventStream.end() here. In MongoDB,
                  // this function apparently is not implemented. This note is just for
                  // informational purposes to ensure that you are aware that the two
                  // implementations differ here.
                };

                onError = function onError(err) {
                  unsubscribe();
                  passThrough.emit('error', err);
                  passThrough.end(); // In the PostgreSQL eventstore, we call eventStream.end() here. In MongoDB,
                  // this function apparently is not implemented. This note is just for
                  // informational purposes to ensure that you are aware that the two
                  // implementations differ here.
                };

                eventStream.on('data', onData);
                eventStream.on('end', onEnd);
                eventStream.on('error', onError);
                return _context6.abrupt("return", passThrough);

              case 10:
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
        var events, i, event, seq;
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
                _context7.prev = 6;
                i = 0;

              case 8:
                if (!(i < events.length)) {
                  _context7.next = 26;
                  break;
                }

                event = events[i];

                if (event.metadata) {
                  _context7.next = 12;
                  break;
                }

                throw new Error('Metadata are missing.');

              case 12:
                if (!(event.metadata.revision === undefined)) {
                  _context7.next = 14;
                  break;
                }

                throw new Error('Revision is missing.');

              case 14:
                if (!(event.metadata.revision < 1)) {
                  _context7.next = 16;
                  break;
                }

                throw new Error('Revision must not be less than 1.');

              case 16:
                _context7.next = 18;
                return this.getNextSequence('events');

              case 18:
                seq = _context7.sent;
                event.data = omitByDeep(event.data, function (value) {
                  return value === undefined;
                });
                event.metadata.position = seq; // Use cloned events here to hinder MongoDB from adding an _id property to
                // the original event objects.

                _context7.next = 23;
                return this.collections.events.insertOne(cloneDeep(event));

              case 23:
                i++;
                _context7.next = 8;
                break;

              case 26:
                _context7.next = 33;
                break;

              case 28:
                _context7.prev = 28;
                _context7.t0 = _context7["catch"](6);

                if (!(_context7.t0.code === 11000 && _context7.t0.message.indexOf('_aggregateId_revision') !== -1)) {
                  _context7.next = 32;
                  break;
                }

                throw new Error('Aggregate id and revision already exist.');

              case 32:
                throw _context7.t0;

              case 33:
                return _context7.abrupt("return", events);

              case 34:
              case "end":
                return _context7.stop();
            }
          }
        }, _callee7, this, [[6, 28]]);
      }));

      function saveEvents(_x6) {
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
        var aggregateId, fromRevision, toRevision;
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
                return this.collections.events.update({
                  'aggregate.id': aggregateId,
                  'metadata.revision': {
                    $gte: fromRevision,
                    $lte: toRevision
                  }
                }, {
                  $set: {
                    'metadata.published': true
                  }
                }, {
                  multi: true
                });

              case 11:
              case "end":
                return _context8.stop();
            }
          }
        }, _callee8, this);
      }));

      function markEventsAsPublished(_x7) {
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
        var snapshot;
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
                return this.collections.snapshots.findOne({
                  aggregateId: aggregateId
                }, {
                  fields: {
                    _id: 0,
                    revision: 1,
                    state: 1
                  }
                });

              case 4:
                snapshot = _context9.sent;

                if (snapshot) {
                  _context9.next = 7;
                  break;
                }

                return _context9.abrupt("return");

              case 7:
                return _context9.abrupt("return", snapshot);

              case 8:
              case "end":
                return _context9.stop();
            }
          }
        }, _callee9, this);
      }));

      function getSnapshot(_x8) {
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
        var aggregateId, revision, state;
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
                return this.collections.snapshots.update({
                  aggregateId: aggregateId
                }, {
                  aggregateId: aggregateId,
                  state: state,
                  revision: revision
                }, {
                  upsert: true
                });

              case 10:
              case "end":
                return _context10.stop();
            }
          }
        }, _callee10, this);
      }));

      function saveSnapshot(_x9) {
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
        var fromPosition, toPosition, passThrough, replayStream, onData, onEnd, onError, unsubscribe;
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
                passThrough = new PassThrough({
                  objectMode: true
                });
                replayStream = this.collections.events.find({
                  $and: [{
                    'metadata.position': {
                      $gte: fromPosition
                    }
                  }, {
                    'metadata.position': {
                      $lte: toPosition
                    }
                  }]
                }, {
                  fields: {
                    _id: 0
                  },
                  sort: 'metadata.position'
                }).stream();

                unsubscribe = function unsubscribe() {
                  replayStream.removeListener('data', onData);
                  replayStream.removeListener('end', onEnd);
                  replayStream.removeListener('error', onError);
                };

                onData = function onData(data) {
                  passThrough.write(Event.wrap(data));
                };

                onEnd = function onEnd() {
                  unsubscribe();
                  passThrough.end(); // In the PostgreSQL eventstore, we call replayStream.end() here. In MongoDB,
                  // this function apparently is not implemented. This note is just for
                  // informational purposes to ensure that you are aware that the two
                  // implementations differ here.
                };

                onError = function onError(err) {
                  unsubscribe();
                  passThrough.emit('error', err);
                  passThrough.end(); // In the PostgreSQL eventstore, we call replayStream.end() here. In MongoDB,
                  // this function apparently is not implemented. This note is just for
                  // informational purposes to ensure that you are aware that the two
                  // implementations differ here.
                };

                replayStream.on('data', onData);
                replayStream.on('end', onEnd);
                replayStream.on('error', onError);
                return _context11.abrupt("return", passThrough);

              case 15:
              case "end":
                return _context11.stop();
            }
          }
        }, _callee11, this);
      }));

      function getReplay(_x10) {
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
                if (!this.client) {
                  _context12.next = 3;
                  break;
                }

                _context12.next = 3;
                return this.client.close(true);

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