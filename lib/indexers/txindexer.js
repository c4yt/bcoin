/*!
 * txindexer.js - tx indexer plugin for bcoin
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const EventEmitter = require('events');

/**
 * @exports indexers/txindexer
 */

const plugin = exports;

/**
 * Plugin
 * @extends EventEmitter
 */

class Plugin extends EventEmitter {
  /**
   * Create a plugin.
   * @constructor
   * @param {Node} node
   */

  constructor(node) {
    super();

    this.config = node.config.filter('indexers');

    this.init();
  }

  init() {
    this.node.on('connect', (entry, block) => {
      console.log('%s (%d) added to chain.', entry.rhash(), entry.height);
    });
  }

  async open() {
  }

  async close() {
  }
}

/**
 * Plugin name.
 * @const {String}
 */

plugin.id = 'txindexer';

/**
 * Plugin initialization.
 * @param {Node} node
 * @returns {WalletDB}
 */

plugin.init = function init(node) {
  return new Plugin(node);
};
