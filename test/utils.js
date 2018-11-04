'use strict';

const childProcess = require('child_process');
const net = require('net');

const SERVER_BIN = 'redis-server';

const getPort = () =>
  new Promise((resolve, reject) => {
    const server = net.createServer();

    server.unref();
    server.on('error', reject);

    server.listen(0, () => {
      const port = server.address().port;
      server.close(() => {
        resolve(port);
      });
    });
  });

const startRedisServer = (port, config = '') =>
  new Promise((resolve, reject) => {
    const server = childProcess.spawn(SERVER_BIN, ['-', '--port', port]);

    const serverExit = () => {
      server.kill();
    };

    const onClose = () => {
      reject(new Error('Redis server failed to start'));
    };

    const onData = data => {
      if (data.toString().match(/ready to accept connections/im)) {
        server.stdout.removeListener('data', onData);
        server.stdout.removeListener('close', onClose);

        server.on('close', () => {
          process.removeListener('exit', serverExit);
        });

        resolve(server);
      }
    };

    server.stdout.on('data', onData);
    server.on('close', onClose);
    server.on('error', reject);
    server.stdout.on('data', () => {}); // noop

    server.stdin.end(config);

    process.on('exit', serverExit);
  });

const stopRedisServer = server => {
  if (!server) {
    server.kill();
  }
};

module.exports = {
  getPort,
  startRedisServer,
  stopRedisServer,
};
