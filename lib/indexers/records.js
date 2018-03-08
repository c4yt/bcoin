/*!
 * records.js - walletdb records
 * Copyright (c) 2014-2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

/**
 * @module wallet/records
 */

const bio = require('bufio');
const util = require('../utils/util');
const consensus = require('../protocol/consensus');

/**
 * Chain State
 */

class IndexState {
  /**
   * Create a chain state.
   * @constructor
   */

  constructor() {
    this.startHeight = 0;
    this.startHash = consensus.NULL_HASH;
    this.height = 0;
  }

  /**
   * Clone the state.
   * @returns {IndexState}
   */

  clone() {
    const state = new IndexState();
    state.startHeight = this.startHeight;
    state.startHash = this.startHash;
    state.height = this.height;
    return state;
  }

  /**
   * Inject properties from serialized data.
   * @private
   * @param {Buffer} data
   */

  fromRaw(data) {
    const br = bio.read(data);

    this.startHeight = br.readU32();
    this.startHash = br.readHash('hex');
    this.height = br.readU32();

    return this;
  }

  /**
   * Instantiate chain state from serialized data.
   * @param {Hash} hash
   * @param {Buffer} data
   * @returns {IndexState}
   */

  static fromRaw(data) {
    return new this().fromRaw(data);
  }

  /**
   * Serialize the chain state.
   * @returns {Buffer}
   */

  toRaw() {
    const bw = bio.write(40);

    bw.writeU32(this.startHeight);
    bw.writeHash(this.startHash);
    bw.writeU32(this.height);

    return bw.render();
  }
}

/**
 * Block Meta
 */

class BlockMeta {
  /**
   * Create block meta.
   * @constructor
   * @param {Hash} hash
   * @param {Number} height
   * @param {Number} time
   */

  constructor(hash, height, time) {
    this.hash = hash || consensus.NULL_HASH;
    this.height = height != null ? height : -1;
    this.time = time || 0;
  }

  /**
   * Clone the block.
   * @returns {BlockMeta}
   */

  clone() {
    return new this.constructor(this.hash, this.height, this.time);
  }

  /**
   * Get block meta hash as a buffer.
   * @returns {Buffer}
   */

  toHash() {
    return Buffer.from(this.hash, 'hex');
  }

  /**
   * Instantiate block meta from chain entry.
   * @private
   * @param {ChainEntry} entry
   */

  fromEntry(entry) {
    this.hash = entry.hash;
    this.height = entry.height;
    this.time = entry.time;
    return this;
  }

  /**
   * Instantiate block meta from json object.
   * @private
   * @param {Object} json
   */

  fromJSON(json) {
    this.hash = util.revHex(json.hash);
    this.height = json.height;
    this.time = json.time;
    return this;
  }

  /**
   * Instantiate block meta from serialized tip data.
   * @private
   * @param {Buffer} data
   */

  fromRaw(data) {
    const br = bio.read(data);
    this.hash = br.readHash('hex');
    this.height = br.readU32();
    this.time = br.readU32();
    return this;
  }

  /**
   * Instantiate block meta from chain entry.
   * @param {ChainEntry} entry
   * @returns {BlockMeta}
   */

  static fromEntry(entry) {
    return new this().fromEntry(entry);
  }

  /**
   * Instantiate block meta from json object.
   * @param {Object} json
   * @returns {BlockMeta}
   */

  static fromJSON(json) {
    return new this().fromJSON(json);
  }

  /**
   * Instantiate block meta from serialized data.
   * @param {Hash} hash
   * @param {Buffer} data
   * @returns {BlockMeta}
   */

  static fromRaw(data) {
    return new this().fromRaw(data);
  }

  /**
   * Serialize the block meta.
   * @returns {Buffer}
   */

  toRaw() {
    const bw = bio.write(42);
    bw.writeHash(this.hash);
    bw.writeU32(this.height);
    bw.writeU32(this.time);
    return bw.render();
  }

  /**
   * Convert the block meta to a more json-friendly object.
   * @returns {Object}
   */

  toJSON() {
    return {
      hash: util.revHex(this.hash),
      height: this.height,
      time: this.time
    };
  }
}

/*
 * Expose
 */

exports.IndexState = IndexState;
exports.BlockMeta = BlockMeta;

module.exports = exports;
