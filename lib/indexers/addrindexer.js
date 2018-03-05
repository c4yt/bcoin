/*!
 * addindexer.js - add indexer plugin for bcoin
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const bdb = require('bdb');
const assert = require('assert');

/**
 * @exports indexers/indexer
 */

const plugin = exports;

plugin.id = 'addrindexer';

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
      T: bdb.key('T', ['hash', 'hash256']),
      C: bdb.key('C', ['hash', 'hash256', 'uint32'])
    };
    this.init();
  }

  init() {
  }

  /**
   * Index a block
   * @param {ChainEntry} entry
   * @param {Block} block
   * @param {CoinView} view
   * @returns {Promise}
   */

  async indexBlock(entry, block, view) {
    const b = this.db.batch();

    for (let i = 0; i < block.txs.length; i++) {
      const tx = block.txs[i];
      const hash = tx.hash();

      for (const addr of tx.getHashes(view))
        b.put(this.layout.T.build(addr, hash), null);

      if (!tx.isCoinbase()) {
        for (const {prevout} of tx.inputs) {
          const {hash, index} = prevout;
          const coin = view.getOutput(prevout);
          assert(coin);

          const addr = coin.getHash();

          if (!addr)
            continue;

          this.del(this.layout.C.build(addr, hash, index));
        }
      }

      for (let j = 0; j < tx.outputs.length; j++) {
        const output = tx.outputs[j];
        const addr = output.getHash();

        if (!addr)
          continue;

        b.put(this.layout.C.build(addr, hash, j), null);
      }
    }

    await b.write();
  }

  /**
   * Unindex a block
   * @param {ChainEntry} entry
   * @param {Block} block
   * @param {CoinView} view
   * @returns {Promise}
   */

  async unindexBlock(entry, block, view) {
    const b = this.db.batch();

    for (let i = 0; i < block.txs.length; i++) {
      const tx = block.txs[i];
      const hash = tx.hash();

      for (const addr of tx.getHashes(view))
        this.del(this.layout.T.build(addr, hash));

      if (!tx.isCoinbase()) {
        for (const {prevout} of tx.inputs) {
          const {hash, index} = prevout;
          const coin = view.getOutput(prevout);
          assert(coin);

          const addr = coin.getHash();

          if (!addr)
            continue;

          b.put(this.layout.C.build(addr, hash, index), null);
        }
      }

      for (let j = 0; j < tx.outputs.length; j++) {
        const output = tx.outputs[j];
        const addr = output.getHash();

        if (!addr)
          continue;

        b.del(this.layout.C.build(addr, hash, j));
      }
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
