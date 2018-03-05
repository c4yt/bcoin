/*!
 * txindexer.js - tx indexer plugin for bcoin
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const bdb = require('bdb');
const TXMeta = require('../primitives/txmeta');

/**
 * @exports indexers/indexer
 */

const plugin = exports;

plugin.id = 'txindexer';

/**
 * Plugin
 * @extends Object
 */

class Plugin extends Object {
  /**
   * Create a plugin.
   * @constructor
   * @param {IndexDB} db
   */

  constructor(db) {
    super();
    this.db = db;
    this.layout =  {
      t: bdb.key('t', ['hash256'])
    };
    this.init();
  }

  init() {
  }

  /**
   * Index a block
   * @param {ChainEntry} entry
   * @param {Block} block
   * @returns {Promise}
   */

  async indexBlock(entry, block) {
    const b = this.db.batch();

    for (let i = 0; i < block.txs.length; i++) {
      const tx = block.txs[i];
      const hash = tx.hash();
      const meta = TXMeta.fromTX(tx, entry, i);
      b.put(this.layout.t.build(hash), meta.toRaw());
    }

    await b.write();
  }

  /**
   * Unindex a block
   * @param {ChainEntry} entry
   * @param {Block} block
   * @returns {Promise}
   */

  async unindexBlock(entry, block) {
    const b = this.db.batch();

    for (let i = 0; i < block.txs.length; i++) {
      const tx = block.txs[i];
      const hash = tx.hash();
      b.del(this.layout.t.build(hash));
    }

    await b.write();
  }
}

/**
 * Plugin initialization.
 * @param {IndexDB} db
 * @returns {WalletDB}
 */

plugin.init = function init(db) {
  return new Plugin(db);
};
