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

  await db.verify(layout.V.build(), 'chain', 4);

  console.log('Updating version to %d.', 5);

  await db.del(layout.V.build());
  await db.verify(layout.V.build(), 'chain', 5);
}

async function removeKey(name, prefix) {
  console.log('Removing %s index', name);

  const iter = db.iterator({
    gte: prefix.min(),
    lte: prefix.max(),
    reverse: true,
    keys: true
  });

  let batch = db.batch();
  let total = 0;

  while (await iter.next()) {
    const {key} = iter;

    batch.del(key);

    if (++total % 10000 === 0) {
      console.log('Cleaned up %d %s index records.', total, name);
      await batch.write();
      batch = db.batch();
    }
  }

  await batch.write();

  console.log('Cleaned up %d %s index records.', total, name);
}

async function removeIndexes() {
  console.log('Removing indexes...');

  removeKey('hash -> tx', layout.t);
  removeKey('addr -> tx', layout.T);
  removeKey('addr -> coin', layout.C);

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
