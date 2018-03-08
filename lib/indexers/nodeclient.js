/*!
 * nodeclient.js - node client for bcoin
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const assert = require('assert');
const AsyncEmitter = require('bevent');

/**
 * Node Client
 * @alias module:node.NodeClient
 */

class NodeClient extends AsyncEmitter {
  /**
   * Create a node client.
   * @constructor
   */

  constructor(node) {
    super();

    this.node = node;
    this.network = node.network;
    this.opened = false;

    this.init();
  }

  /**
   * Initialize the client.
   */

  init() {
    this.node.chain.on('connect', (entry, block, view) => {
      if (!this.opened)
        return;

      this.emit('block connect', entry, block, view);
    });

    this.node.chain.on('disconnect', (entry, block, view) => {
      if (!this.opened)
        return;

      this.emit('block disconnect', entry, block, view);
    });

    this.node.on('tx', (tx) => {
      if (!this.opened)
        return;

      this.emit('tx', tx);
    });

    this.node.on('reset', (tip) => {
      if (!this.opened)
        return;

      this.emit('chain reset', tip);
    });
  }

  /**
   * Open the client.
   * @returns {Promise}
   */

  async open(options) {
    assert(!this.opened, 'NodeClient is already open.');
    this.opened = true;
    setImmediate(() => this.emit('connect'));
  }

  /**
   * Close the client.
   * @returns {Promise}
   */

  async close() {
    assert(this.opened, 'NodeClient is not open.');
    this.opened = false;
    setImmediate(() => this.emit('disconnect'));
  }

  /**
   * Add a listener.
   * @param {String} type
   * @param {Function} handler
   */

  bind(type, handler) {
    return this.on(type, handler);
  }

  /**
   * Add a listener.
   * @param {String} type
   * @param {Function} handler
   */

  hook(type, handler) {
    return this.on(type, handler);
  }
}

/*
 * Expose
 */

module.exports = NodeClient;
