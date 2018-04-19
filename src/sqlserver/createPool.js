'use strict';

const { Connection } = require('tedious'),
      genericPool = require('generic-pool');

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

  const { host, port, user, password, database } = config;

  const pool = genericPool.createPool({
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

          pool.emit('error', err);
        };

        handleEnd = () => {
          removeAllListeners();

          if (!hasBeenConnected) {
            return reject(new Error('Could not connect to database.'));
          }

          pool.emit('disconnect');
        };

        connection.on('connect', handleConnect);
        connection.on('error', handleError);
        connection.on('end', handleEnd);
      });
    },

    destroy (connection) {
      if (connection.closed) {
        return Promise.resolve();
      }

      return new Promise(resolve => {
        connection.removeAllListeners('end');
        connection.removeAllListeners('error');

        connection.once('end', () => {
          resolve();
        });

        connection.close();
      });
    }
  });

  pool.on('factoryCreateError', err => {
    throw err;
  });

  pool.on('factoryDestroyError', err => {
    throw err;
  });

  return pool;
};

module.exports = createPool;
