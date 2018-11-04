'use strict';

const childProcess = require('child_process');
const path = require('path');
const Redis = require('ioredis');
const { Readable, Writable } = require('stream');

const pkg = require('../package');
const { getPort, startRedisServer, stopRedisServer } = require('../test/utils');

const dumperBin = path.join(__dirname, '../bin/redis-dumper');

function execCli(...args) {
  return new Promise((resolve, reject) => {
    childProcess.exec(`${process.execPath} "${dumperBin}" ${args.join(' ')}`, (error, stdout, stderr) => {
      if (error) {
        reject(Object.assign(new Error(error.message), { stdout, stderr }));
      } else {
        const stdoutLines = stdout
          .toString()
          .split('\n')
          .map(line => line && line.trim())
          .filter(Boolean);

        const stderrLines = stderr
          .toString()
          .split('\n')
          .map(line => line && line.trim())
          .filter(Boolean)
          .map(line => `ERR: ${line}`);

        resolve(stdoutLines.concat(stderrLines));
      }
    });
  });
}

const mockDump = jest.fn();
const mockMinimist = jest.fn();

jest.mock('./dump', () => mockDump);
jest.mock('minimist', () => mockMinimist);

describe('CLI', () => {
  describe('Module', () => {
    let mockArgs = {};

    beforeEach(() => {
      mockArgs = {};
      mockDump.mockClear();
      mockDump.mockImplementation(
        () =>
          new Readable({
            read() {
              process.nextTick(() => this.push(null));
            },
          }),
      );
      mockMinimist.mockClear();
      mockMinimist.mockImplementation(() => mockArgs);
      jest.resetModules();
    });

    test('should show help', async () => {
      jest.spyOn(console, 'log').mockImplementation(() => {});
      mockArgs.help = true;

      require('./cli');

      expect(console.log).toHaveBeenCalled();
      expect(console.log.mock.calls[0][0]).toMatch(`${pkg.name} ${pkg.version}`);

      console.log.mockRestore();
    });

    test('should dump current database', () => {
      require('./cli');

      expect(mockDump).toBeCalledWith({ auth: null, db: 0, filter: '*', host: '127.0.0.1', port: 6379 });
    });

    test('should report error when dump fails', done => {
      const spies = [
        jest.spyOn(console, 'error').mockImplementation(() => {}),
        jest.spyOn(process, 'stdout', 'get').mockImplementation(
          () =>
            new Writable({
              write(data, encoding, callback) {
                callback();
              },
            }),
        ),
        jest.spyOn(process, 'exit').mockImplementation(() => {
          expect(console.error).toHaveBeenCalledTimes(2);
          expect(process.exit).toBeCalledWith(1);
          spies.forEach(spy => spy.mockRestore());
          done();
        }),
      ];
      const mockStream = new Readable({
        read() {
          process.nextTick(() => this.emit('error', new Error('Test error')));
        },
      });

      mockDump.mockImplementation(() => mockStream);

      require('./cli');

      expect(mockDump).toBeCalledWith({ auth: null, db: 0, filter: '*', host: '127.0.0.1', port: 6379 });
    });
  });

  describe('Command line', () => {
    const host = 'localhost';
    let port = 0;
    let server = null;
    let client = null;

    beforeEach(async () => {
      port = await getPort();

      server = await startRedisServer(port);
      client = new Redis(host, port);
    });

    afterEach(() => {
      stopRedisServer(server);
      if (client !== null) {
        client.disconnect();
      }
    });

    test('should show help', async () => {
      const output = await execCli('--help');

      expect(output[0]).toEqual(`${pkg.name} ${pkg.version}`);
    });

    test('should create empty dump for empty database', async () => {
      const output = await execCli('-p', port);

      expect(output).toHaveLength(0); // empty database
    });

    test('should create database dump', async () => {
      await client.set('key', 'value');

      const output = await execCli('-p', port);

      expect(output).toEqual(['*3', '$3', 'SET', '$3', 'key', '$5', 'value']);
    });

    test('should apply filter to keys', async () => {
      await client.set('my:key', 'value');
      await client.set('other:key', 'value');

      const output = await execCli('-p', port, '-f', 'my:*');

      expect(output).toEqual(['*3', '$3', 'SET', '$6', 'my:key', '$5', 'value']);
    });

    test('should select correct db', async () => {
      await client.select(1);
      await client.set('key1', 'value');
      await client.select(0);
      await client.set('key', 'value');

      const output = await execCli('-p', port, '-d', '1');

      expect(output).toEqual(['*3', '$3', 'SET', '$4', 'key1', '$5', 'value']);
    });

    test('should authenticate', async () => {
      await client.set('key', 'value');
      await client.config('set', 'requirepass', 'foobar');

      try {
        await client.get('key');
        throw new Error('Expected NOAUTH Error');
      } catch (e) {
        expect(e.message).toMatch('NOAUTH');
      }

      const output = await execCli('-p', port, '-a', 'foobar');

      expect(output).toEqual(['*3', '$3', 'SET', '$3', 'key', '$5', 'value']);
    });

    test('should handle connection error', async () => {
      expect(await execCli('-p', port, '-a', 'foobar')).toEqual([
        'ERR: [WARN] Redis server does not require a password, but a password was supplied.',
      ]);

      await expect(execCli('-p', 12345)).rejects.toThrow(/connect ECONNREFUSED 127.0.0.1:12345/);
    });
  });
});
