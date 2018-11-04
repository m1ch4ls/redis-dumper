'use strict';

const argv = require('minimist')(process.argv.slice(2));
const { Writable } = require('stream');
const pump = require('pump');

const dump = require('./dump');
const pkg = require('../package.json');

if (argv.help) {
  console.log(`
  ${pkg.name} ${pkg.version}

  Usage: redis-dumper [OPTIONS]
    -h <hostname>    Server hostname (default: 127.0.0.1)
    -p <port>        Server port (default: 6379)
    -d <db>          Database number (default: 0)
    -a <auth>        Password
    -f <filter>      Query filter (default: *)
    --help           Output this help and exit

  Examples:
    redis-dumper
    redis-dumper -p 6500
    redis-dumper -f 'mydb:*' > mydb.dump.db

  The output is a valid list of redis commands.
  That means the following will work:
    redis-dumper > dump.db      # Dump redis database
    redis-cli --pipe < dump.db  # Import redis database from generated file
`);
  process.exit(0);
} else {
  // redirect console to error to prevent mixing output with error messages
  console.log = console.error;
  console.warn = console.error;

  pump(
    dump({
      filter: 'f' in argv ? argv.f : '*',
      db: 'd' in argv ? argv.d : 0,
      port: 'p' in argv ? argv.p : 6379,
      auth: 'a' in argv ? argv.a : null,
      host: 'h' in argv ? argv.h : '127.0.0.1',
    }),
    // trick to prevent stdout from closing
    new Writable({
      write(chunk, encoding, callback) {
        process.stdout.write(chunk, encoding, callback);
      },
    }),
    error => {
      if (error) {
        console.error('Dump failed with an error:');
        console.error(error.message);
        process.exit(1);
        return;
      }

      process.exit(0);
    },
  );
}
