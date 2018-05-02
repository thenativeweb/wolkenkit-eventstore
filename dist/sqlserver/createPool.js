'use strict';

var _promise = require('babel-runtime/core-js/promise');

var _promise2 = _interopRequireDefault(_promise);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var _require = require('tedious'),
    Connection = _require.Connection,
    _require2 = require('tarn'),
    Pool = _require2.Pool;

var createPool = function createPool(_ref) {
  var host = _ref.host,
      port = _ref.port,
      user = _ref.user,
      password = _ref.password,
      database = _ref.database,
      encrypt = _ref.encrypt,
      onError = _ref.onError,
      onDisconnect = _ref.onDisconnect;

  if (!host) {
    throw new Error('Host is missing.');
  }
  if (!port) {
    throw new Error('Port is missing.');
  }
  if (!user) {
    throw new Error('User is missing.');
  }
  if (!password) {
    throw new Error('Password is missing.');
  }
  if (!database) {
    throw new Error('Database is missing.');
  }
  if (encrypt === undefined) {
    throw new Error('Encrypt is missing.');
  }

  onError = onError || function () {
    // Intentionally left blank.
  };

  onDisconnect = onDisconnect || function () {
    // Intentionally left blank.
  };

  var pool = new Pool({
    min: 2,
    max: 10,
    acquireTimeoutMillis: 1000,
    createTimeoutMillis: 1000,
    idleTimeoutMillis: 1000,
    propagateCreateError: true,

    validate: function validate(connection) {
      return !connection.closed;
    },
    create: function create() {
      return new _promise2.default(function (resolve, reject) {
        var connection = new Connection({
          server: host,
          options: { port: port, encrypt: encrypt },
          userName: user,
          password: password,
          database: database
        });

        var handleConnect = void 0,
            handleEnd = void 0,
            handleError = void 0,
            hasBeenConnected = false;

        var unsubscribe = function unsubscribe() {
          connection.removeListener('connect', handleConnect);
          connection.removeListener('error', handleError);
          connection.removeListener('end', handleEnd);
        };

        var unsubscribeSetup = function unsubscribeSetup() {
          connection.removeListener('connect', handleConnect);
        };

        handleConnect = function handleConnect(err) {
          if (err) {
            unsubscribe();

            return reject(err);
          }

          hasBeenConnected = true;
          unsubscribeSetup();
          resolve(connection);
        };

        handleError = function handleError(err) {
          unsubscribe();

          onError(err);
        };

        handleEnd = function handleEnd() {
          unsubscribe();

          if (!hasBeenConnected) {
            return reject(new Error('Could not connect to database.'));
          }

          onDisconnect();
        };

        connection.on('connect', handleConnect);
        connection.on('error', handleError);
        connection.on('end', handleEnd);
      });
    },
    destroy: function destroy(connection) {
      if (connection.closed) {
        return;
      }

      connection.removeAllListeners('end');
      connection.removeAllListeners('error');

      connection.close();
    }
  });

  return pool;
};

module.exports = createPool;