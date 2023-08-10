const {
  getDataFromEthArchive,
  getLogsRequestBodyEthArchive,
} = require("./eth-archive-queries.js");

const {
  getDataFromSkar,
  getLogsRequestBodySkar,
} = require("./skar-queries.js");

const { getLogsWithTxsAndBlocks } = require("./rpc-queries.js");

const { ethers } = require("ethers");

const assert = require("assert");

//Takes the block data from eth archive and eth rpc
//and sanitizes them for comparison
const getCommontBlockData = (blockDataObj) => {
  const {
    sha3Uncles,
    miner,
    stateRoot,
    transactionsRoot,
    receiptsRoot,
    logsBloom,
    difficulty,
    number,
    gasLimit,
    gasUsed,
    timestamp,
    extraData,
    nonce,
    totalDifficulty,
    size,
    hash,
  } = blockDataObj;

  return {
    sha3Uncles,
    miner,
    stateRoot,
    transactionsRoot,
    receiptsRoot,
    logsBloom,
    //toBeHex will normalize numbers, strings, BigInts and
    //hex value
    difficulty: ethers.toBeHex(difficulty),
    number: ethers.toBeHex(number),
    gasLimit,
    gasUsed,
    timestamp,
    extraData,
    nonce: ethers.toBeHex(nonce),
    totalDifficulty: ethers.toBeHex(totalDifficulty),
    size,
    hash,
  };
};

//Takes the transaction data from eth archive and eth rpc
//and sanitizes them for comparison
const getCommontTxData = (txDataObj) => {
  const {
    type,
    nonce,
    to,
    gas,
    value,
    input,
    v,
    r,
    s,
    from,
    blockHash,
    blockNumber,
    index,
    gasPrice,
    hash,
    status,
    transactionIndex,
  } = txDataObj;

  const actualIndex = ethers.toBeHex(index ?? transactionIndex);

  return {
    //toBeHex will normalize numbers, strings, BigInts and
    //hex value
    type: ethers.toBeHex(type),
    nonce: ethers.toBeHex(nonce),
    to,
    gas: ethers.toBeHex(gas),
    value: ethers.toBeHex(value),
    input,
    v: ethers.toBeHex(v),
    r: ethers.toBeHex(r),
    s: ethers.toBeHex(s),
    from,
    blockHash,
    blockNumber: ethers.toBeHex(blockNumber),
    index: actualIndex,
    gasPrice: ethers.toBeHex(gasPrice),
    hash,
    status: ethers.toBeHex(status),
  };
};

//Takes the log data from eth archive and eth rpc
//and sanitizes them for comparison
const getCommontLogData = (logDataObj) => {
  const {
    address,
    blockHash,
    blockNumber,
    data,
    index,
    removed,
    transactionHash,
    transactionIndex,
    logIndex,
  } = logDataObj;

  const actualIndex = ethers.toBeHex(index ?? logIndex);

  return {
    address,
    data,
    removed,
    transactionHash,
    blockHash,
    //toBeHex will normalize numbers, strings, BigInts and
    //hex value
    blockNumber: ethers.toBeHex(blockNumber),
    transactionIndex: ethers.toBeHex(transactionIndex),
    index: actualIndex,
  };
};

const main = async () => {
  //Test changing the parameters below
  const START_BLOCK = 11001621;
  const END_BLOCK = 11007622;
  const ADDRESSES = ["0x3883f5e181fccaF8410FA61e12b59BAd963fb645"];
  const TOPICS = [
    ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
  ];

  let skarDataPromise = getDataFromSkar(
    (from_block, to_block) =>
      getLogsRequestBodySkar(from_block, to_block, ADDRESSES, TOPICS),
    START_BLOCK,
    END_BLOCK,
  );

  let ethArchiveDataPromise = getDataFromEthArchive(
    (fromBlock, toBlock) =>
      getLogsRequestBodyEthArchive(fromBlock, toBlock, ADDRESSES, TOPICS),
    START_BLOCK,
    END_BLOCK
  );

  let rpcDataPromise = getLogsWithTxsAndBlocks(
    START_BLOCK,
    END_BLOCK,
    ADDRESSES,
    TOPICS
  );

  const [skarData, ethArchiveData, rpcData] = await Promise.all([
    skarDataPromise,
    ethArchiveDataPromise,
    rpcDataPromise,
  ]);

  const skarComparisonData = {
    blocks: [],
    transactions: [],
    logs: [],
  };

  const ethArchiveComparisonData = {
    blocks: [],
    transactions: [],
    logs: [],
  };

  const rpcComparisonData = {
    blocks: [],
    transactions: [],
    logs: [],
  };

  const camelize = item => {
    if (Array.isArray(item)) {
      return item.map(el => camelize(el));
    } else if (typeof item === 'function' || item !== Object(item)) {
      return item;
    }
    return Object.fromEntries(
      Object.entries(item).map(([key, value]) => [
        key.replace(/([-_][a-z])/gi, c => c.toUpperCase().replace(/[-_]/g, '')),
        camelize(value),
      ]),
    );
  };

  skarData.forEach((data) => {
    data.logs.forEach((log) => {
      skarComparisonData.logs.push(getCommontLogData(camelize(log)));
    });
    data.transactions.forEach((tx) => {
      skarComparisonData.transactions.push(getCommontTxData(camelize(tx)));
    });
    data.blocks.forEach((block) => {
      skarComparisonData.blocks.push(getCommontBlockData(camelize(block)));
    });
  });

  ethArchiveData.forEach((outerArr) => {
    outerArr.forEach((blockData) => {
      ethArchiveComparisonData.blocks.push(
        getCommontBlockData(blockData.block)
      );

      blockData.transactions.forEach((tx) =>
        ethArchiveComparisonData.transactions.push(getCommontTxData(tx))
      );
      blockData.logs.forEach((log) =>
        ethArchiveComparisonData.logs.push(getCommontLogData(log))
      );
    });
  });

  rpcData.blocks.forEach((block) => {
    rpcComparisonData.blocks.push(getCommontBlockData(block));
  });

  rpcData.transactions.forEach((transaction) => {
    rpcComparisonData.transactions.push(getCommontTxData(transaction));
  });

  rpcData.logs.forEach((log) => {
    rpcComparisonData.logs.push(getCommontLogData(log));
  });

  assert.equal(rpcComparisonData.blocks.length, skarComparisonData.blocks.length);

  assert.deepStrictEqual(ethArchiveComparisonData, rpcComparisonData);
  assert.deepStrictEqual(ethArchiveComparisonData, skarComparisonData);
  console.log("All queries match");
};

main();
