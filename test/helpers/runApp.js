'use strict';

const fork = require('child_process').fork;

const apps = [];

const cleanUpAndExit = function () {
  if (apps.length > 0) {
    apps.forEach(app => app.kill('SIGINT'));
  }

  /* eslint-disable no-process-exit */
  process.exit(0);
  /* eslint-enable no-process-exit */
};

const runApp = function (options, callback) {
  if (!options) {
    throw new Error('Options are missing.');
  }
  if (!options.app) {
    throw new Error('App is missing.');
  }
  if (!callback) {
    throw new Error('Callback is missing.');
  }

  options.env = options.env || {};

  const app = fork(options.app, {
    env: options.env
  });

  apps.push(app);

  app.on('close', code => {
    const index = apps.indexOf(app);

    apps.splice(index, 1);

    if (code !== 0) {
      return callback(new Error(`Exited with status code ${code}.`));
    }
    callback(null);
  });
};

process.on('SIGINT', cleanUpAndExit);
process.on('SIGTERM', cleanUpAndExit);

module.exports = runApp;
