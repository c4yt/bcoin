'use strict';

const bcoin = require('../..');
const Index = require('bindex/lib/index');

// Create a blockchain and store it in leveldb.
// `db` also accepts `rocksdb` and `lmdb`.
const chain = new bcoin.Chain({
  memory: true,
  network: 'testnet'
});

const mempool = new bcoin.Mempool({ chain: chain });

// Create a network pool of peers with a limit of 8 peers.
const pool = new bcoin.Pool({
  chain: chain,
  mempool: mempool,
  maxPeers: 8
});

const index = new Index({
  memory: true,
  network: 'testnet',
  chain: chain,
  indexTX: true,
  indexAddress: true
});

// Open the pool (implicitly opens mempool and chain).
(async function() {
  await pool.open();

  // Connect, start retrieving and relaying txs
  await pool.connect();

  // Start the blockchain sync.
  pool.startSync();

  await chain.open();

  await index.open();

  console.log('Current height:', chain.height);

  // Watch the action
  chain.on('block', (block) => {
    console.log('block: %s', block.rhash());
  });

  mempool.on('tx', (tx) => {
    console.log('tx: %s', tx.rhash);
  });

  pool.on('tx', (tx) => {
    console.log('tx: %s', tx.rhash);
  });

  await pool.stopSync();

  const tip = await index.getTip();
  const block = await chain.getBlock(tip.hash);
  const meta = await index.getMeta(block.txs[0].hash());
  const tx = meta.tx;
  const coinview = await chain.db.getSpentView(meta);

  console.log(`Tx with hash ${tx.hash()}:`, meta);
  console.log(`Tx input: ${tx.getInputValue(coinview)},` +
    ` output: ${tx.getOutputValue()}, fee: ${tx.getFee(coinview)}`);
})();
