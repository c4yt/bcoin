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
const {BloomFilter} = require('bfilter');
const CoinView = require('../coins/coinview');
const Network = require('../protocol/network');
const NullClient = require('./nullclient');
const layout = require('./layout');
const {BlockMeta} = require('./records');

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
    this.tip = new BlockMeta();
    this.lock = new Lock();
    this.filter = new BloomFilter();

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

    if (tip.height < this.tip.height) {
      this.logger.warning(
        'IndexDB is connecting low blocks (%d).',
        tip.height);
      return 0;
    }

    if (tip.height >= this.network.block.slowHeight)
      this.logger.debug('Adding block: %d.', tip.height);

    if (tip.height === this.tip.height) {
      // We let blocks of the same height
      // through specifically for rescans:
      // we always want to rescan the last
      // block since the tip may have
      // updated before the block was fully
      // processed (in the case of a crash).
      this.logger.warning('Already saw IndexDB block (%d).', tip.height);
    } else if (tip.height !== this.tip.height + 1) {
      await this.scan(this.tip.height);
      return 0;
    }

    // Sync the new tip.
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

    if (tip.height > this.tip.height) {
      this.logger.warning(
        'IndexDB is disconnecting high blocks (%d).',
        tip.height);
      return 0;
    }

    if (tip.height !== this.tip.height)
      throw new Error('IDB: Bad disconnection (height mismatch).');

    const prevEntry = await this.client.getEntry(tip.height - 1);
    const prev = BlockMeta.fromEntry(prevEntry);
    assert(prev);

    // Sync the previous tip.
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
   * Send filter to the remote node.
   * @private
   * @returns {Promise}
   */

  syncFilter() {
    this.logger.info('Sending filter to server (%dmb).',
      this.filter.size / 8 / (1 << 20));

    this.filterSent = true;
    return this.client.setFilter(this.filter);
  }

  /**
   * Sync the current chain tip.
   * @param {BlockMeta} tip
   * @returns {Promise}
   */

  async setTip(tip) {
    const b = this.db.batch();
    // Save tip.
    b.put(layout.h.build(), tip.toRaw());
    await b.write();

    this.tip = tip;
  }

  /**
   * Sync tip with server on every connect.
   * @returns {Promise}
   */

  async syncNode() {
    const unlock = await this.lock.lock();
    try {
      this.logger.info('Resyncing from server...');

      this.tip = await this.getTip();
      this.logger.info(
        'IndexDB loaded (height=%d, hash=%s).',
        this.tip.height,
        this.tip.hash);

      await this.syncFilter();
      await this.syncChain();
    } finally {
      unlock();
    }
  }

  /**
   * Connect and sync with the chain server.
   * @private
   * @returns {Promise}
   */

  async syncChain() {
    this.logger.info('Syncing state from height %d.', this.tip.height);

    const best = await this.client.getTip();
    for (let height = this.tip.height + 1;
         height <= best.height; height++) {
      this.logger.info('scanning block %d', height);

      const entry = await this.client.getEntry(height);
      const block = await this.client.getBlock(entry.hash);
      // TODO: maintain coinview
      const view = new CoinView();
      await this.indexBlock(entry, block, view);
    }
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
   * Get tip.
   * @param {Hash} hash
   * @returns {Promise}
   */

  async getTip() {
    const raw = await this.db.get(layout.h.build());

    if (!raw) {
      this.setTip(this.tip);
      return this.tip;
    }

    const tip = BlockMeta.fromRaw(raw);
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
    if (height > this.tip.height)
      throw new Error('IDB: Cannot rollback to the future.');

    if (height === this.tip.height) {
      this.logger.info('Rolled back to same height (%d).', height);
      return;
    }

    this.logger.info(
      'Rolling back %d IndexDB blocks to height %d.',
      this.tip.height - height, height);

    const tip = await this.getBlock(height);
    assert(tip);

    const iter = this.db.iterator({
      gte: layout.t.build(tip.height + 1),
      lte: layout.t.max(),
      reverse: true,
      keys: true
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
