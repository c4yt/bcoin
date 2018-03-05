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
const layout = require('./layout');
const records = require('./records');
const Network = require('../protocol/network');

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
    this.node = options.node;
    this.state = new IndexState();
    this.lock = new Lock();
    this.plugins = Object.create(null);
    this.stack = [];

    this.init();
  }

  /**
   * Initialize indexdb.
   * @private
   */

  init() {
    this._bind();

    for (const indexer of this.options.indexers) {
      const plugin = require('./' + indexer + 'indexer');
      this.use(plugin);
    }
  }

  /**
   * Attach a plugin.
   * @param {Object} plugin
   * @returns {Object} Plugin instance.
   */

  use(plugin) {
    assert(plugin, 'Plugin must be an object.');
    assert(typeof plugin.init === 'function', '`init` must be a function.');

    const instance = plugin.init(this.db);

    assert(!instance.indexBlock || typeof instance.indexBlock === 'function',
      '`indexBlock` must be a function.');
    assert(!instance.unindexBlock
            || typeof instance.unindexBlock === 'function',
      '`unindexBlock` must be a function.');

    if (plugin.id) {
      assert(typeof plugin.id === 'string', '`id` must be a string.');

      assert(!this.plugins[plugin.id], `${plugin.id} is already added.`);

      this.plugins[plugin.id] = instance;
    }

    this.stack.push(instance);

    return instance;
  }

  /**
   * Test whether a plugin is available.
   * @param {String} name
   * @returns {Boolean}
   */

  has(name) {
    return this.plugins[name] != null;
  }

  /**
   * Get a plugin.
   * @param {String} name
   * @returns {Object|null}
   */

  get(name) {
    assert(typeof name === 'string', 'Plugin name must be a string.');

    return this.plugins[name] || null;
  }

  /**
   * Require a plugin.
   * @param {String} name
   * @returns {Object}
   * @throws {Error} on onloaded plugin
   */

  require(name) {
    const plugin = this.get(name);
    assert(plugin, `${name} is not loaded.`);
    return plugin;
  }

  /**
   * Bind to node events.
   * @private
   */

  _bind() {
    this.node.on('connect', async (entry, block) => {
      try {
        for (const plugin of this.stack)
          await plugin.indexBlock(entry, block);
      } catch (e) {
        this.emit('error', e);
      }
    });

    this.node.on('disconnect', async (entry, block) => {
      try {
        for (const plugin of this.stack)
          await plugin.unindexBlock(entry, block);
      } catch (e) {
        this.emit('error', e);
      }
    });
  }

  /**
   * Open the indexdb, wait for the database to load.
   * @returns {Promise}
   */

  async open() {
    await this.db.open();
    await this.db.verify(layout.V.build(), 'indexers', 7);

    await this.verifyNetwork();

    this.logger.info(
      'Indexers loaded (height=%d, start=%d).',
      this.state.height,
      this.state.startHeight);
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
   * Close the indexdb, wait for the database to close.
   * @returns {Promise}
   */

  async close() {
    return this.db.close();
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
    const hashes = await this.node.chain.getHashes(-1, -1);

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
    state.marked = false;

    b.put(layout.R.build(), state.toRaw());

    await b.write();

    this.state = state;
    this.height = state.height;

    return undefined;
  }

  /**
   * Migrate sync state.
   * @private
   * @param {ChainState} state
   * @returns {Promise}
   */

  async migrateState(state) {
    const b = this.db.batch();

    this.logger.info('Migrating to new sync state.');

    const hashes = await this.node.chain.getHashes(0, state.height);

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

      if (await this.node.chain.getEntry(tip.hash))
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

    this.logger.info(
      'IndexDB is scanning %d blocks.',
      this.state.height - height + 1);

    // Index blocks starting from height
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
      state.marked = false;
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
    state.marked = true;

    const b = this.db.batch();
    b.put(layout.R.build(), state.toRaw());
    await b.write();

    this.state = state;
    this.height = state.height;
  }

  /**
   * Get index tip.
   * @param {Hash} hash
   * @returns {Promise}
   */

  async getTip() {
    const tip = await this.getBlock(this.state.height);

    if (!tip)
      throw new Error('IDB: Tip not found!');

    return tip;
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

    if (options.indexers != null) {
      assert(typeof options.indexers === 'object');
      for (const indexer of options.indexers) {
        const plugin = require('./' + indexer + 'indexer');
        assert(plugin);
      }
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
