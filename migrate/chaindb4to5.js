'use strict';

const assert = require('assert');
const bio = require('bufio');
const bdb = require('bdb');
const Network = require('../lib/protocol/network');
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

/**
 * ChainFlags
 */

class ChainFlags {
  constructor(options) {
    this.network = Network.primary;
    this.spv = false;
    this.witness = true;
    this.prune = false;
    this.indexTX = false;
    this.indexAddress = false;
    this.bip91 = false;
    this.bip148 = false;

    if (options)
      this.fromOptions(options);
  }

  fromRaw(data) {
    const br = bio.read(data);

    this.network = Network.fromMagic(br.readU32());

    const flags = br.readU32();

    this.spv = (flags & 1) !== 0;
    this.witness = (flags & 2) !== 0;
    this.prune = (flags & 4) !== 0;
    this.indexTX = (flags & 8) !== 0;
    this.indexAddress = (flags & 16) !== 0;
    this.bip91 = (flags & 32) !== 0;
    this.bip148 = (flags & 64) !== 0;

    return this;
  }

  static fromRaw(data) {
    return new ChainFlags().fromRaw(data);
  }
}

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

/*
 * Execute
 */

(async () => {
  await db.open();

  console.log('Opened %s.', process.argv[2]);

  await updateVersion();

  const data = await db.get(layout.O.build());
  const flags = ChainFlags.fromRaw(data);

  if (!flags.indexTX && !flags.indexAddress) {
    await db.close();
    return;
  }

  parent = db.batch();
  if (flags.indexTX)
    removeKey('hash -> tx', layout.t);

  if (flags.indexAddress) {
    removeKey('addr -> tx', layout.T);
    removeKey('addr -> coin', layout.C);
  }
  await parent.write();

  await db.close();
})().then(() => {
  console.log('Migration complete.');
  process.exit(0);
}).catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
