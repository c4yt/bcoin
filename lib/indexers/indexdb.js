/*!
 * indexdb.js - storage for indexers
 * Copyright (c) 2014-2015, Fedor Indutny (MIT License)
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const assert = require('assert');
const path = require('path');
const EventEmitter = require('events');
const bdb = require('bdb');
const Logger = require('blgr');
const {Lock} = require('bmutex');
const Network = require('../protocol/network');
const CoinView = require('../coins/coinview');
const NullClient = require('./nullclient');
const layout = require('./layout');
const records = require('./records');

const {
  IndexState,
  BlockMeta
} = records;

/**
 * IndexDB
 * @alias module:index.IndexDB
 * @extends EventEmitter
 */

class IndexDB extends EventEmitter {
  /**
   * Create a index db.
   * @constructor
   * @param {Object} options
   */

  constructor(options) {
    super();

    this.options = new IndexOptions(options);
    this.logger = this.options.logger.context('indexers');
    this.network = this.options.network;
    this.db = bdb.create(this.options);
    this.client = this.options.client || new NullClient(this);
    this.state = new IndexState();
    this.lock = new Lock();

    this.init();
  }

  /**
   * Initialize indexdb.
   * @private
   */

  init() {
    this._bind();
  }

  /**
   * Bind to node events.
   * @private
   */

  _bind() {
    this.client.on('error', (err) => {
      this.emit('error', err);
    });

    this.client.on('connect', async () => {
      try {
        await this.syncNode();
      } catch (e) {
        this.emit('error', e);
      }
    });

    this.client.bind('block connect', async (entry, block, view) => {
      try {
        await this.indexBlock(entry, block, view);
      } catch (e) {
        this.emit('error', e);
      }
    });

    this.client.bind('block disconnect', async (entry, block, view) => {
      try {
        await this.unindexBlock(entry, block, view);
      } catch (e) {
        this.emit('error', e);
      }
    });

    this.client.hook('block rescan', async (entry, txs) => {
      try {
        await this.rescanBlock(entry, txs);
      } catch (e) {
        this.emit('error', e);
      }
    });

    this.client.bind('tx', async (tx) => {
      try {
        await this.addTX(tx);
      } catch (e) {
        this.emit('error', e);
      }
    });

    this.client.bind('chain reset', async (tip) => {
      try {
        await this.resetChain(tip);
      } catch (e) {
        this.emit('error', e);
      }
    });
  }

  /**
   * Index a block
   * @param (ChainEntry) - entry
   * @param (Block) - block
   * @param (CoinView) - view
   * @returns {Promise}
   */

  async indexBlock(entry, block, view) {
    const tip = BlockMeta.fromEntry(entry);

    if (tip.height < this.state.height) {
      this.logger.warning(
        'IndexDB is connecting low blocks (%d).',
        tip.height);
      return 0;
    }

    if (tip.height >= this.network.block.slowHeight)
      this.logger.debug('Adding block: %d.', tip.height);

    if (tip.height === this.state.height) {
      // We let blocks of the same height
      // through specifically for rescans:
      // we always want to rescan the last
      // block since the state may have
      // updated before the block was fully
      // processed (in the case of a crash).
      this.logger.warning('Already saw IndexDB block (%d).', tip.height);
    } else if (tip.height !== this.state.height + 1) {
      await this.scan(this.state.height);
      return 0;
    }

    // Sync the state to the new tip.
    await this.setTip(tip);
    return 0;
  }

  /**
   * Unindex a block
   * @param (ChainEntry) - entry
   * @param (Block) - block
   * @param (CoinView) - view
   * @returns {Promise}
   */

  async unindexBlock(entry, block, view) {
    const tip = BlockMeta.fromEntry(entry);

    if (tip.height === 0)
      throw new Error('IDB: Bad disconnection (genesis block).');

    if (tip.height > this.state.height) {
      this.logger.warning(
        'IndexDB is disconnecting high blocks (%d).',
        tip.height);
      return 0;
    }

    if (tip.height !== this.state.height)
      throw new Error('IDB: Bad disconnection (height mismatch).');

    const prev = await this.getBlock(tip.height - 1);
    assert(prev);

    // Sync the state to the previous tip.
    await this.setTip(prev);
    return 0;
  }

  /**
   * Verify network.
   * @returns {Promise}
   */

  async verifyNetwork() {
    const raw = await this.db.get(layout.O.build());

    if (!raw) {
      const b = this.db.batch();
      b.put(layout.O.build(), fromU32(this.network.magic));
      return b.write();
    }

    const magic = raw.readUInt32LE(0, true);

    if (magic !== this.network.magic)
      throw new Error('Network mismatch for IndexDB.');

    return undefined;
  }

  /**
   * Open the indexdb, wait for the database to load.
   * @returns {Promise}
   */

  async open() {
    await this.db.open();
    await this.db.verify(layout.V.build(), 'indexers', 0);

    await this.verifyNetwork();

    await this.connect();

    this.logger.info(
      'IndexDB loaded (height=%d, start=%d).',
      this.state.height,
      this.state.startHeight);
  }

  /**
   * Close the indexdb, wait for the database to close.
   * @returns {Promise}
   */

  async close() {
    return this.db.close();
  }

  /**
   * Connect to the node server (client required).
   * @returns {Promise}
   */

  async connect() {
    return this.client.open();
  }

  /**
   * Disconnect from node server (client required).
   * @returns {Promise}
   */

  async disconnect() {
    return this.client.close();
  }

  /**
   * Get the best block hash.
   * @returns {Promise}
   */

  async getState() {
    const data = await this.db.get(layout.R.build());

    if (!data)
      return null;

    return IndexState.fromRaw(data);
  }

  /**
   * Sync the current chain state to tip.
   * @param {BlockMeta} tip
   * @returns {Promise}
   */

  async setTip(tip) {
    const b = this.db.batch();
    const state = this.state.clone();

    if (tip.height < state.height) {
      // Hashes ahead of our new tip
      // that we need to delete.
      while (state.height !== tip.height) {
        b.del(layout.h.build(state.height));
        state.height -= 1;
      }
    } else if (tip.height > state.height) {
      assert(tip.height === state.height + 1, 'Bad chain sync.');
      state.height += 1;
    }

    if (tip.height < state.startHeight) {
      state.startHeight = tip.height;
      state.startHash = tip.hash;
    }

    // Save tip and state.
    b.put(layout.h.build(tip.height), tip.toHash());
    b.put(layout.R.build(), state.toRaw());

    await b.write();

    this.state = state;
    this.height = state.height;
  }

  /**
   * Mark current state.
   * @param {BlockMeta} block
   * @returns {Promise}
   */

  async markState(block) {
    const state = this.state.clone();
    state.startHeight = block.height;
    state.startHash = block.hash;

    const b = this.db.batch();
    b.put(layout.R.build(), state.toRaw());
    await b.write();

    this.state = state;
    this.height = state.height;
  }

  /**
   * Sync state with server on every connect.
   * @returns {Promise}
   */

  async syncNode() {
    const unlock = await this.lock.lock();
    try {
      this.logger.info('Resyncing from server...');
      await this.syncState();
      await this.syncChain();
    } finally {
      unlock();
    }
  }

  /**
   * Initialize and write initial sync state.
   * @returns {Promise}
   */

  async syncState() {
    const cache = await this.getState();

    if (cache) {
      if (!await this.getBlock(0))
        return this.migrateState(cache);

      this.state = cache;
      this.height = cache.height;

      return undefined;
    }

    this.logger.info('Initializing database state from server.');

    const b = this.db.batch();
    const hashes = await this.client.getHashes();

    let tip = null;

    for (let height = 0; height < hashes.length; height++) {
      const hash = hashes[height];
      const meta = new BlockMeta(hash, height);
      b.put(layout.h.build(height), meta.toHash());
      tip = meta;
    }

    assert(tip);

    const state = this.state.clone();
    state.startHeight = tip.height;
    state.startHash = tip.hash;
    state.height = tip.height;

    b.put(layout.R.build(), state.toRaw());

    await b.write();

    this.state = state;
    this.height = state.height;

    return undefined;
  }

  /**
   * Migrate sync state.
   * @private
   * @param {IndexState} state
   * @returns {Promise}
   */

  async migrateState(state) {
    const b = this.db.batch();

    this.logger.info('Migrating to new sync state.');

    const hashes = await this.client.getHashes(0, state.height);

    for (let height = 0; height < hashes.length; height++) {
      const hash = hashes[height];
      const meta = new BlockMeta(hash, height);
      b.put(layout.h.build(height), meta.toHash());
    }

    await b.write();

    this.state = state;
    this.height = state.height;
  }

  /**
   * Connect and sync with the chain server.
   * @private
   * @returns {Promise}
   */

  async syncChain() {
    let height = this.state.height;

    this.logger.info('Syncing state from height %d.', height);

    for (;;) {
      const tip = await this.getBlock(height);
      assert(tip);

      if (await this.client.getEntry(tip.hash))
        break;

      assert(height !== 0);
      height -= 1;
    }

    return this.scan(height);
  }

  /**
   * Rescan blockchain from a given height.
   * @private
   * @param {Number?} height
   * @returns {Promise}
   */

  async scan(height) {
    if (height == null)
      height = this.state.startHeight;

    assert((height >>> 0) === height, 'IDB: Must pass in a height.');

    await this.rollback(height);

    this.logger.info(
      'IndexDB is scanning %d blocks.',
      this.state.height - height + 1);

    const tip = await this.getTip();

    this.logger.info('Scanning from height %d.', tip.height);

    let entry = await this.client.getEntry(tip.height);

    let total = 0;
    while (entry) {
      const block = await this.client.getBlock(entry.hash);

      total +=1;
      const view = new CoinView();
      await this.indexBlock(entry, block, view);

      entry = await this.client.getNext(entry);
    }

    this.logger.info('Finished scanning %d blocks.', total);
  }

  /**
   * Force a rescan.
   * @param {Number} height
   * @returns {Promise}
   */

  async rescan(height) {
    const unlock = await this.lock.lock();
    try {
      return await this._rescan(height);
    } finally {
      unlock();
    }
  }

  /**
   * Force a rescan (without a lock).
   * @private
   * @param {Number} height
   * @returns {Promise}
   */

  async _rescan(height) {
    return this.scan(height);
  }

  /**
   * Get a block meta.
   * @param {Hash} hash
   * @returns {Promise}
   */

  async getBlock(height) {
    const data = await this.db.get(layout.h.build(height));

    if (!data)
      return null;

    const block = new BlockMeta();
    block.hash = data.toString('hex');
    block.height = height;

    return block;
  }

  /**
   * Get tip.
   * @param {Hash} hash
   * @returns {Promise}
   */

  async getTip() {
    const tip = await this.getBlock(this.state.height);

    if (!tip)
      throw new Error('IDB: Tip not found!');

    return tip;
  }

  /**
   * Sync with chain height.
   * @param {Number} height
   * @returns {Promise}
   */

  async rollback(height) {
    if (height > this.state.height)
      throw new Error('IDB: Cannot rollback to the future.');

    if (height === this.state.height) {
      this.logger.info('Rolled back to same height (%d).', height);
      return;
    }

    this.logger.info(
      'Rolling back %d IndexDB blocks to height %d.',
      this.state.height - height, height);

    const tip = await this.getBlock(height);
    assert(tip);

    const iter = this.db.iterator({
      gte: layout.t.build(tip.height + 1),
      lte: layout.t.max(),
      reverse: true,
      values: true
    });

    const batch = this.db.batch();
    let total = 0;

    while (await iter.next()) {
      const {key} = iter;
      batch.del(key);
      total += 1;
    }
    await batch.write();

    this.logger.info('Rolled back %d IndexDB blocks.', total);
    await this.setTip(tip);
  }
}

/**
 * Index Options
 * @alias module:index.IndexOptions
 */

class IndexOptions {
  /**
   * Create index options.
   * @constructor
   * @param {Object} options
   */

  constructor(options) {
    this.network = Network.primary;
    this.logger = Logger.global;
    this.client = null;
    this.indexers = [];
    this.prefix = null;
    this.location = null;
    this.memory = true;
    this.maxFiles = 64;
    this.cacheSize = 16 << 20;
    this.compression = true;

    if (options)
      this.fromOptions(options);
  }

  /**
   * Inject properties from object.
   * @private
   * @param {Object} options
   * @returns {IndexOptions}
   */

  fromOptions(options) {
    if (options.network != null)
      this.network = Network.get(options.network);

    if (options.logger != null) {
      assert(typeof options.logger === 'object');
      this.logger = options.logger;
    }

    if (options.client != null) {
      assert(typeof options.client === 'object');
      this.client = options.client;
    }

    if (options.indexers != null) {
      assert(typeof options.indexers === 'object');
      this.indexers = options.indexers;
    }

    if (options.prefix != null) {
      assert(typeof options.prefix === 'string');
      this.prefix = options.prefix;
      this.location = path.join(this.prefix, 'index');
    }

    if (options.location != null) {
      assert(typeof options.location === 'string');
      this.location = options.location;
    }

    if (options.memory != null) {
      assert(typeof options.memory === 'boolean');
      this.memory = options.memory;
    }

    if (options.maxFiles != null) {
      assert((options.maxFiles >>> 0) === options.maxFiles);
      this.maxFiles = options.maxFiles;
    }

    if (options.cacheSize != null) {
      assert(Number.isSafeInteger(options.cacheSize) && options.cacheSize >= 0);
      this.cacheSize = options.cacheSize;
    }

    if (options.compression != null) {
      assert(typeof options.compression === 'boolean');
      this.compression = options.compression;
    }

    return this;
  }

  /**
   * Instantiate chain options from object.
   * @param {Object} options
   * @returns {IndexOptions}
   */

  static fromOptions(options) {
    return new this().fromOptions(options);
  }
}

/*
 * Helpers
 */

function fromU32(num) {
  const data = Buffer.allocUnsafe(4);
  data.writeUInt32LE(num, 0, true);
  return data;
}

/*
 * Expose
 */

module.exports = IndexDB;
