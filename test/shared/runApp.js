'use strict';

const { fork } = require('child_process');

const apps = [];

const cleanUpAndExit = function () {
  if (apps.length > 0) {
    apps.forEach(app => app.kill('SIGINT'));
  }

  /* eslint-disable no-process-exit */
  process.exit(0);
  /* eslint-enable no-process-exit */
};

const runApp = function ({ app, env = {}}) {
  return new Promise((resolve, reject) => {
    if (!app) {
      throw new Error('App is missing.');
    }

    const childProcess = fork(app, { env });

    apps.push(childProcess);

    childProcess.on('close', code => {
      const index = apps.indexOf(childProcess);

      apps.splice(index, 1);

      if (code !== 0) {
        return reject(new Error(`Exited with status code ${code}.`));
      }
      resolve();
    });
  });
};

process.on('SIGINT', cleanUpAndExit);
process.on('SIGTERM', cleanUpAndExit);

module.exports = runApp;
