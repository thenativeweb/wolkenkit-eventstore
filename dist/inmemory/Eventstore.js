'use strict';

var _toConsumableArray2 = require('babel-runtime/helpers/toConsumableArray');

var _toConsumableArray3 = _interopRequireDefault(_toConsumableArray2);

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
    flatten = require('lodash/flatten');


var omitByDeep = require('../omitByDeep');

var Eventstore = function (_EventEmitter) {
  (0, _inherits3.default)(Eventstore, _EventEmitter);

  function Eventstore() {
    (0, _classCallCheck3.default)(this, Eventstore);
    return (0, _possibleConstructorReturn3.default)(this, (Eventstore.__proto__ || (0, _getPrototypeOf2.default)(Eventstore)).apply(this, arguments));
  }

  (0, _createClass3.default)(Eventstore, [{
    key: 'initialize',
    value: function () {
      var _ref = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee() {
        return _regenerator2.default.wrap(function _callee$(_context) {
          while (1) {
            switch (_context.prev = _context.next) {
              case 0:
                this.database = {
                  events: [],
                  snapshots: []
                };

              case 1:
              case 'end':
                return _context.stop();
            }
          }
        }, _callee, this);
      }));

      function initialize() {
        return _ref.apply(this, arguments);
      }

      return initialize;
    }()
  }, {
    key: 'getStoredEvents',
    value: function getStoredEvents() {
      return this.database.events;
    }
  }, {
    key: 'getStoredSnapshots',
    value: function getStoredSnapshots() {
      return this.database.snapshots;
    }
  }, {
    key: 'storeEventAtDatabase',
    value: function storeEventAtDatabase(event) {
      this.database.events.push(event);
    }
  }, {
    key: 'storeSnapshotAtDatabase',
    value: function storeSnapshotAtDatabase(snapshot) {
      this.database.snapshots.push(snapshot);
    }
  }, {
    key: 'updateEventInDatabaseAtIndex',
    value: function updateEventInDatabaseAtIndex(index, newEventData) {
      this.database.events[index] = newEventData;
    }
  }, {
    key: 'getLastEvent',
    value: function () {
      var _ref2 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee2(aggregateId) {
        var eventsInDatabase, lastEvent;
        return _regenerator2.default.wrap(function _callee2$(_context2) {
          while (1) {
            switch (_context2.prev = _context2.next) {
              case 0:
                if (aggregateId) {
                  _context2.next = 2;
                  break;
                }

                throw new Error('Aggregate id is missing.');

              case 2:
                eventsInDatabase = this.getStoredEvents().filter(function (event) {
                  return event.aggregate.id === aggregateId;
                });

                if (!(eventsInDatabase.length === 0)) {
                  _context2.next = 5;
                  break;
                }

                return _context2.abrupt('return');

              case 5:
                lastEvent = eventsInDatabase[eventsInDatabase.length - 1];
                return _context2.abrupt('return', Event.wrap(lastEvent));

              case 7:
              case 'end':
                return _context2.stop();
            }
          }
        }, _callee2, this);
      }));

      function getLastEvent(_x) {
        return _ref2.apply(this, arguments);
      }

      return getLastEvent;
    }()
  }, {
    key: 'getEventStream',
    value: function () {
      var _ref3 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee3(aggregateId, options) {
        var fromRevision, toRevision, passThrough, filteredEvents;
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

                options = options || {};

                fromRevision = options.fromRevision || 1;
                toRevision = options.toRevision || Math.pow(2, 31) - 1;

                if (!(fromRevision > toRevision)) {
                  _context3.next = 7;
                  break;
                }

                throw new Error('From revision is greater than to revision.');

              case 7:
                passThrough = new PassThrough({ objectMode: true });
                filteredEvents = this.getStoredEvents().filter(function (event) {
                  return event.aggregate.id === aggregateId && event.metadata.revision >= fromRevision && event.metadata.revision <= toRevision;
                });


                filteredEvents.forEach(function (event) {
                  passThrough.write(Event.wrap(event));
                });
                passThrough.end();

                return _context3.abrupt('return', passThrough);

              case 12:
              case 'end':
                return _context3.stop();
            }
          }
        }, _callee3, this);
      }));

      function getEventStream(_x2, _x3) {
        return _ref3.apply(this, arguments);
      }

      return getEventStream;
    }()
  }, {
    key: 'getUnpublishedEventStream',
    value: function () {
      var _ref4 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee4() {
        var filteredEvents, passThrough;
        return _regenerator2.default.wrap(function _callee4$(_context4) {
          while (1) {
            switch (_context4.prev = _context4.next) {
              case 0:
                filteredEvents = this.getStoredEvents().filter(function (event) {
                  return event.metadata.published === false;
                });
                passThrough = new PassThrough({ objectMode: true });


                filteredEvents.forEach(function (event) {
                  passThrough.write(Event.wrap(event));
                });
                passThrough.end();

                return _context4.abrupt('return', passThrough);

              case 5:
              case 'end':
                return _context4.stop();
            }
          }
        }, _callee4, this);
      }));

      function getUnpublishedEventStream() {
        return _ref4.apply(this, arguments);
      }

      return getUnpublishedEventStream;
    }()
  }, {
    key: 'saveEvents',
    value: function () {
      var _ref6 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee5(_ref5) {
        var _this2 = this;

        var events = _ref5.events;
        var eventsInDatabase;
        return _regenerator2.default.wrap(function _callee5$(_context5) {
          while (1) {
            switch (_context5.prev = _context5.next) {
              case 0:
                if (events) {
                  _context5.next = 2;
                  break;
                }

                throw new Error('Events are missing.');

              case 2:
                if (!(Array.isArray(events) && events.length === 0)) {
                  _context5.next = 4;
                  break;
                }

                throw new Error('Events are missing.');

              case 4:

                events = cloneDeep(flatten([events]));

                eventsInDatabase = this.getStoredEvents();


                events.forEach(function (event) {
                  if (!event.metadata) {
                    throw new Error('Metadata are missing.');
                  }
                  if (event.metadata.revision === undefined) {
                    throw new Error('Revision is missing.');
                  }
                  if (event.metadata.revision < 1) {
                    throw new Error('Revision must not be less than 1.');
                  }

                  if (eventsInDatabase.find(function (eventInDatabase) {
                    return event.aggregate.id === eventInDatabase.aggregate.id && event.metadata.revision === eventInDatabase.metadata.revision;
                  })) {
                    throw new Error('Aggregate id and revision already exist.');
                  }

                  var newPosition = eventsInDatabase.length + 1;

                  event.data = omitByDeep(event.data, function (value) {
                    return value === undefined;
                  });
                  event.metadata.position = newPosition;

                  _this2.storeEventAtDatabase(event);
                });

                return _context5.abrupt('return', events);

              case 8:
              case 'end':
                return _context5.stop();
            }
          }
        }, _callee5, this);
      }));

      function saveEvents(_x4) {
        return _ref6.apply(this, arguments);
      }

      return saveEvents;
    }()
  }, {
    key: 'markEventsAsPublished',
    value: function () {
      var _ref8 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee6(_ref7) {
        var aggregateId = _ref7.aggregateId,
            fromRevision = _ref7.fromRevision,
            toRevision = _ref7.toRevision;
        var eventsFromDatabase, shouldEventBeMarkedAsPublished, i, event, eventToUpdate;
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
                if (fromRevision) {
                  _context6.next = 4;
                  break;
                }

                throw new Error('From revision is missing.');

              case 4:
                if (toRevision) {
                  _context6.next = 6;
                  break;
                }

                throw new Error('To revision is missing.');

              case 6:
                if (!(fromRevision > toRevision)) {
                  _context6.next = 8;
                  break;
                }

                throw new Error('From revision is greater than to revision.');

              case 8:
                eventsFromDatabase = this.getStoredEvents();

                shouldEventBeMarkedAsPublished = function shouldEventBeMarkedAsPublished(event) {
                  return event.aggregate.id === aggregateId && event.metadata.revision >= fromRevision && event.metadata.revision <= toRevision;
                };

                for (i = 0; i < eventsFromDatabase.length; i++) {
                  event = eventsFromDatabase[i];


                  if (shouldEventBeMarkedAsPublished(event)) {
                    eventToUpdate = cloneDeep(event);


                    eventToUpdate.metadata.published = true;
                    this.updateEventInDatabaseAtIndex(i, eventToUpdate);
                  }
                }

              case 11:
              case 'end':
                return _context6.stop();
            }
          }
        }, _callee6, this);
      }));

      function markEventsAsPublished(_x5) {
        return _ref8.apply(this, arguments);
      }

      return markEventsAsPublished;
    }()
  }, {
    key: 'getSnapshot',
    value: function () {
      var _ref9 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee7(aggregateId) {
        var matchingSnapshotsForAggregateId, newestSnapshotRevision, matchingSnapshot;
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
                matchingSnapshotsForAggregateId = this.getStoredSnapshots().filter(function (snapshot) {
                  return snapshot.aggregateId === aggregateId;
                });
                newestSnapshotRevision = Math.max.apply(Math, (0, _toConsumableArray3.default)(matchingSnapshotsForAggregateId.map(function (snapshot) {
                  return snapshot.revision;
                })));
                matchingSnapshot = matchingSnapshotsForAggregateId.find(function (snapshot) {
                  return snapshot.revision === newestSnapshotRevision;
                });

                if (matchingSnapshot) {
                  _context7.next = 7;
                  break;
                }

                return _context7.abrupt('return');

              case 7:
                return _context7.abrupt('return', {
                  revision: matchingSnapshot.revision,
                  state: matchingSnapshot.state
                });

              case 8:
              case 'end':
                return _context7.stop();
            }
          }
        }, _callee7, this);
      }));

      function getSnapshot(_x6) {
        return _ref9.apply(this, arguments);
      }

      return getSnapshot;
    }()
  }, {
    key: 'saveSnapshot',
    value: function () {
      var _ref11 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee8(_ref10) {
        var aggregateId = _ref10.aggregateId,
            revision = _ref10.revision,
            state = _ref10.state;
        var snapshot;
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
                if (revision) {
                  _context8.next = 4;
                  break;
                }

                throw new Error('Revision is missing.');

              case 4:
                if (state) {
                  _context8.next = 6;
                  break;
                }

                throw new Error('State is missing.');

              case 6:

                state = omitByDeep(state, function (value) {
                  return value === undefined;
                });

                snapshot = {
                  aggregateId: aggregateId,
                  revision: revision,
                  state: state
                };


                this.storeSnapshotAtDatabase(snapshot);

              case 9:
              case 'end':
                return _context8.stop();
            }
          }
        }, _callee8, this);
      }));

      function saveSnapshot(_x7) {
        return _ref11.apply(this, arguments);
      }

      return saveSnapshot;
    }()
  }, {
    key: 'getReplay',
    value: function () {
      var _ref12 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee9(options) {
        var fromPosition, toPosition, passThrough, filteredEvents;
        return _regenerator2.default.wrap(function _callee9$(_context9) {
          while (1) {
            switch (_context9.prev = _context9.next) {
              case 0:
                options = options || {};

                fromPosition = options.fromPosition || 1;
                toPosition = options.toPosition || Math.pow(2, 31) - 1;

                if (!(fromPosition > toPosition)) {
                  _context9.next = 5;
                  break;
                }

                throw new Error('From position is greater than to position.');

              case 5:
                passThrough = new PassThrough({ objectMode: true });
                filteredEvents = this.getStoredEvents().filter(function (event) {
                  return event.metadata.position >= fromPosition && event.metadata.position <= toPosition;
                });


                filteredEvents.forEach(function (event) {
                  passThrough.write(Event.wrap(event));
                });
                passThrough.end();

                return _context9.abrupt('return', passThrough);

              case 10:
              case 'end':
                return _context9.stop();
            }
          }
        }, _callee9, this);
      }));

      function getReplay(_x8) {
        return _ref12.apply(this, arguments);
      }

      return getReplay;
    }()

    /* eslint-disable*/

  }, {
    key: 'destroy',
    value: function () {
      var _ref13 = (0, _asyncToGenerator3.default)( /*#__PURE__*/_regenerator2.default.mark(function _callee10() {
        return _regenerator2.default.wrap(function _callee10$(_context10) {
          while (1) {
            switch (_context10.prev = _context10.next) {
              case 0:
                this.database = {
                  events: [],
                  snapshots: []
                };

              case 1:
              case 'end':
                return _context10.stop();
            }
          }
        }, _callee10, this);
      }));

      function destroy() {
        return _ref13.apply(this, arguments);
      }

      return destroy;
    }()
    /* eslint-enable*/

  }]);
  return Eventstore;
}(EventEmitter);

module.exports = Eventstore;