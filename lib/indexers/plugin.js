/*!
 * plugin.js - indexer plugin for bcoin
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const EventEmitter = require('events');
const IndexDB = require('./indexdb');

/**
 * @exports indexers/indexer
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

    this.network = node.network;
    this.logger = node.logger;
    this.config = node.config.filter('indexers');

    this.idb = new IndexDB({
      indexers: this.config.array('enabled'),
      node: node,
      network: this.network,
      logger: this.logger,
      prefix: this.config.prefix,
      memory: this.config.bool('memory', node.memory),
      maxFiles: this.config.uint('max-files'),
      cacheSize: this.config.mb('cache-size')
    });

    this.init();
  }

  init() {
    this.idb.on('error', err => this.emit('error', err));
  }

  async open() {
    await this.idb.open();
  }

  async close() {
    await this.idb.close();
  }
}

/**
 * Plugin name.
 * @const {String}
 */

plugin.id = 'indexer';

/**
 * Plugin initialization.
 * @param {Node} node
 * @returns {WalletDB}
 */

plugin.init = function init(node) {
  return new Plugin(node);
};
