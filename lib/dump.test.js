'use strict';

const bln = require('big-list-of-naughty-strings');
const crypto = require('crypto');
const childProcess = require('child_process');
const Redis = require('ioredis');
const concatStream = require('concat-stream');
const net = require('net');

const dump = require('./dump');

const SERVER_BIN = 'redis-server';
const CLI_BIN = 'redis-cli';

jest.setTimeout(60000);

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

const startRedisServer = port =>
  new Promise((resolve, reject) => {
    const server = childProcess.spawn(SERVER_BIN, ['--port', port]);

    const onClose = () => {
      reject(new Error('Redis server failed to start'));
    };

    const onData = data => {
      if (data.toString().match(/ready to accept connections/im)) {
        server.stdout.removeListener('data', onData);
        server.stdout.removeListener('close', onClose);
        resolve(server);
      }
    };

    server.stdout.on('data', onData);
    server.on('close', onClose);
    server.on('error', reject);
    server.stdout.on('data', () => {}); // noop
  });

describe('RedisDumper', () => {
  const host = 'localhost';
  let port = 0;
  let server = null;
  let client = null;

  const serverExit = () => {
    if (server !== null) {
      server.kill();
      server = null;
    }
  };

  const redisCli = input =>
    new Promise((resolve, reject) => {
      const cli = childProcess.spawn(CLI_BIN, ['-p', port, '--pipe']);

      cli.stdin.write(input);
      cli.stdin.end();

      const stdout = [];
      const stderr = [];
      cli.stdout.on('data', data => stdout.push(data.toString()));
      cli.stderr.on('data', data => stderr.push(data.toString()));

      const onClose = code => {
        if (code !== 0) {
          reject(new Error(`redis-cli failed with error code ${code}\n${stderr.join('')}`));
        } else {
          resolve(stdout.join(''));
        }
      };

      cli.on('close', onClose);
      cli.on('error', reject);
    });

  const getDump = () =>
    new Promise(resolve => {
      dump({ port }).pipe(concatStream(resolve));
    });

  beforeEach(async () => {
    port = await getPort();

    server = await startRedisServer(port);
    client = new Redis(host, port);

    process.on('exit', serverExit);
  });

  afterEach(() => {
    serverExit();
    process.removeListener('exit', serverExit);
    if (client !== null) {
      client.disconnect();
    }
  });

  test('should dump string', async () => {
    // feed in strings
    for (const str of bln) {
      await client.set(str, str);

      // sanity check
      const value = await client.get(str);
      expect(value).toEqual(str);
    }

    const result = await getDump();

    await client.flushdb();

    await redisCli(result);

    for (const str of bln) {
      const value = await client.get(str);
      expect(value).toEqual(str);
    }
  });

  test('should dump list', async () => {
    const data = [];
    const key = 'key';
    for (let i = 0; i < 100; i++) {
      data.push(crypto.randomBytes(20));
    }

    await client.rpushBuffer(key, data);

    const result = await getDump();

    await client.flushdb();

    await redisCli(result);

    expect(await client.llen(key)).toBe(100);
    const list = await client.lrangeBuffer(key, 0, -1);

    for (let i = 0; i < 100; i++) {
      expect(Buffer.compare(data[i], Buffer.from(list[i]))).toBe(0);
    }
  });

  test('should dump set', async () => {
    const data = [];
    const key = 'key';
    for (let i = 0; i < 100; i++) {
      data.push(crypto.randomBytes(20));
    }

    await client.saddBuffer(key, data);

    const result = await getDump();

    await client.flushdb();

    await redisCli(result);

    const set = await client.smembersBuffer(key);

    data.sort(Buffer.compare);
    set.sort(Buffer.compare);
    for (let i = 0; i < 100; i++) {
      expect(Buffer.compare(data[i], Buffer.from(set[i]))).toBe(0);
    }
  });

  test('should dump sorted set', async () => {
    const data = [];
    const key = 'key';
    for (let i = 0; i < 100; i++) {
      data.push(Buffer.from(String(i)));
      data.push(crypto.randomBytes(20));
    }

    await client.zaddBuffer(key, data);

    const result = await getDump();

    await client.flushdb();

    await redisCli(result);

    const set = await client.zrangeBuffer(key, 0, -1, 'withscores');

    data.sort(Buffer.compare);
    set.sort(Buffer.compare);
    for (let i = 0; i < 100; i++) {
      expect(Buffer.compare(data[i], Buffer.from(set[i]))).toBe(0);
    }
  });

  test('should dump hash', async () => {
    const data = [];
    const key = 'key';
    for (let i = 0; i < 100; i++) {
      data.push(crypto.randomBytes(10));
      data.push(crypto.randomBytes(10));
    }

    await client.hmsetBuffer(key, data);

    const result = await getDump();

    await client.flushdb();

    await redisCli(result);

    const hash = await client.hgetallBuffer(key);

    data.sort(Buffer.compare);
    hash.sort(Buffer.compare);
    for (let i = 0; i < 100; i++) {
      expect(Buffer.compare(data[i], Buffer.from(hash[i]))).toBe(0);
    }
  });
});
