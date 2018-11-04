'use strict';

const { Readable } = require('stream');
const Redis = require('ioredis');

const Command = Redis.Command;

Command.setReplyTransformer('hgetall', result => result);

const cmd = (...args) => new Command(...args).toWritable();

/**
 * @param {Buffer} key
 * @param {string} type
 * @param {Buffer|Buffer[]} value
 * @param {number} ttl
 * @return {Buffer}
 */
const outputProtocol = (key, type, value, ttl) => {
  const commands = [];
  switch (type) {
    case 'string':
      commands.push(cmd('SET', [key, value]));
      break;
    case 'list':
      commands.push(cmd('DEL', [key]));
      commands.push(cmd('RPUSH', [key, ...value]));
      break;
    case 'set':
      commands.push(cmd('DEL', [key]));
      commands.push(cmd('SADD', [key, ...value]));
      break;
    case 'zset':
      commands.push(cmd('DEL', [key]));
      commands.push(cmd('ZADD', [key, ...value.reverse()]));
      break;
    case 'hash': {
      commands.push(cmd('DEL', [key]));
      commands.push(cmd('HMSET', [key, ...value]));
      break;
    }
    default:
    // do nothing
  }
  const expire = parseInt(ttl, 10);
  if (!Number.isNaN(expire) && expire !== -1) {
    commands.push(cmd('EXPIRE', [key, expire]));
  }

  return Buffer.concat(commands);
};

class RedisDumper extends Readable {
  constructor({ port = 6379, host = '127.0.0.1', db = '0', filter = '*', auth = null } = {}) {
    super();

    this.emitError = this.emit.bind(this, 'error');

    this.options = {
      host,
      port,
      db,
      password: auth,
      stringNumbers: true,
      dropBufferSupport: false,
    };
    this.redis = null;
    this.scanStream = null;
    this.scanDrained = false;
    this.filter = filter;
    this.dumping = 0;
    this.keys = [];

    this.once('end', () => {
      this.destroy();
    });
  }

  setupScanStream() {
    this.redis = new Redis(this.options);
    this.redis.on('error', this.emitError);

    const scanStream = this.redis.scanBufferStream({
      match: this.filter,
      count: 100,
    });
    scanStream.on('error', this.emitError);
    scanStream.on('data', keys => {
      this.keys = keys;
      scanStream.pause();
      this.nextKey();
    });
    scanStream.on('end', () => {
      this.scanDrained = true;
      this.nextKey();
    });

    this.scanStream = scanStream;
  }

  nextKey() {
    if (this.keys.length > 0) {
      return this.dumpKey(this.keys.pop());
    } else if (this.scanStream === null) {
      return this.setupScanStream();
    }

    if (!this.scanDrained && this.scanStream.isPaused()) {
      this.scanStream.resume();
    } else if (this.scanDrained && this.dumping === 0) {
      super.push(null); // end stream
    }
  }

  dumpKey(key) {
    this.dumping += 1;
    this.redis
      .type(key)
      .then(type => Promise.all([type, this.loadValue(key, type), this.redis.ttl(key)]))
      .then(([type, value, ttl]) => this.push(outputProtocol(key, type, value, ttl)))
      .catch(this.emitError);
  }

  loadValue(key, type) {
    switch (type) {
      case 'string':
        return this.redis.getBuffer(key);
      case 'list':
        return this.redis.lrangeBuffer(key, 0, -1);
      case 'set':
        return this.redis.smembersBuffer(key);
      case 'zset':
        return this.redis.zrangeBuffer(key, 0, -1, 'withscores');
      case 'hash':
        return this.redis.hgetallBuffer(key);
      default:
        throw new Error('Unhandled type');
    }
  }

  push(value) {
    this.dumping -= 1;
    super.push(value);
  }

  _read() {
    this.nextKey();
  }

  _destroy() {
    if (this.redis !== null) {
      this.redis.disconnect(false);
      this.redis = null;
    }
  }
}

module.exports = params => new RedisDumper(params);
module.exports.RedisDumper = RedisDumper;
