'use strict';

const assert = require('assert');
const bdb = require('bdb');
const layout = require('../lib/blockchain/layout');

// changes:
// rm indexes

assert(process.argv.length > 2, 'Please pass in a database path.');

let parent = null;

const db = bdb.create({
  location: process.argv[2],
  memory: false,
  compression: true,
  cacheSize: 32 << 20,
  createIfMissing: false
});

async function updateVersion() {
  console.log('Checking version.');

  const data = await db.get(layout.V.build());
  assert(data, 'No version.');

  const ver = data.readUInt32LE(0, true);

  if (ver !== 4)
    throw Error(`DB is version ${ver}.`);

  console.log('Updating version to %d.', ver + 1);

  const buf = Buffer.allocUnsafe(5 + 4);
  buf.write('chain', 0, 'ascii');
  buf.writeUInt32LE(4, 6, true);

  parent.put(layout.V.build(), buf);
}

async function removeIndexes() {
  console.log('Removing indexes...');

  let iter = this.db.iterator({
    gte: layout.t.min(),
    lte: layout.t.max(),
    reverse: true,
    keys: true
  });

  let batch = this.db.batch();

  while (await iter.next()) {
    const {key} = iter;
    batch.del(key);
  }
  await batch.write();

  iter = this.db.iterator({
    gte: layout.T.min(),
    lte: layout.T.max(),
    reverse: true,
    keys: true
  });

  batch = this.db.batch();

  while (await iter.next()) {
    const {key} = iter;
    batch.del(key);
  }
  await batch.write();

  iter = this.db.iterator({
    gte: layout.C.min(),
    lte: layout.C.max(),
    reverse: true,
    keys: true
  });

  batch = this.db.batch();

  while (await iter.next()) {
    const {key} = iter;
    batch.del(key);
  }
  await batch.write();

  console.log('Removed indexes');
}

/*
 * Execute
 */

(async () => {
  await db.open();

  console.log('Opened %s.', process.argv[2]);

  parent = db.batch();

  await updateVersion();
  await removeIndexes();

  await parent.write();
  await db.close();
})().then(() => {
  console.log('Migration complete.');
  process.exit(0);
}).catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
