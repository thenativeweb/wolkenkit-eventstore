'use strict';

const { Connection } = require('tedious'),
      { Pool } = require('tarn');

const createPool = function (config) {
  if (!config) {
    throw new Error('Config is missing.');
  }
  if (!config.host) {
    throw new Error('Host is missing.');
  }
  if (!config.port) {
    throw new Error('Port is missing.');
  }
  if (!config.user) {
    throw new Error('User is missing.');
  }
  if (!config.password) {
    throw new Error('Password is missing.');
  }
  if (!config.database) {
    throw new Error('Database is missing.');
  }

  const { host,
    port,
    user,
    password,
    database,
    onError = () => {
      // noop
    },
    onDisconnect = () => {
      // noop
    } } = config;

  const pool = new Pool({
    min: 2,
    max: 10,
    acquireTimeoutMillis: 1000,
    createTimeoutMillis: 1000,
    idleTimeoutMillis: 1000,
    propagateCreateError: true,

    validate (connection) {
      if (connection.closed) {
        return false;
      }

      return true;
    },

    create () {
      return new Promise((resolve, reject) => {
        const connection = new Connection({
          server: host,
          options: {
            port
          },
          userName: user,
          password,
          database
        });

        let handleConnect,
            handleEnd,
            handleError,
            hasBeenConnected = false;

        const removeAllListeners = () => {
          connection.removeListener('connect', handleConnect);
          connection.removeListener('error', handleError);
          connection.removeListener('end', handleEnd);
        };

        const removeOnlySetupListeners = () => {
          connection.removeListener('connect', handleConnect);
        };

        handleConnect = err => {
          if (err) {
            removeAllListeners();

            return reject(err);
          }

          hasBeenConnected = true;
          removeOnlySetupListeners();
          resolve(connection);
        };

        handleError = err => {
          removeAllListeners();

          onError(err);
        };

        handleEnd = () => {
          removeAllListeners();

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

    destroy (connection) {
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
