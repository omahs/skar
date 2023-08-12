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

const run_benchmark = async (start_block, end_block, addresses, topics) => {
  let skarData = await getDataFromSkar(
    (from_block, to_block) =>
      getLogsRequestBodySkar(from_block, to_block, addresses, topics),
    start_block,
    end_block,
  );

  let ethArchiveData = await getDataFromEthArchive(
    (fromBlock, toBlock) =>
      getLogsRequestBodyEthArchive(fromBlock, toBlock, addresses, topics),
    start_block,
    end_block
  );

  console.log("finished benchmark");
};

const run_test = async (start_block, end_block, addresses, topics) => {
  let skarDataPromise = getDataFromSkar(
    (from_block, to_block) =>
      getLogsRequestBodySkar(from_block, to_block, addresses, topics),
    start_block,
    end_block,
  );

  let ethArchiveDataPromise = getDataFromEthArchive(
    (fromBlock, toBlock) =>
      getLogsRequestBodyEthArchive(fromBlock, toBlock, addresses, topics),
    start_block,
    end_block
  );

  let rpcDataPromise = getLogsWithTxsAndBlocks(
    start_block,
    end_block,
    addresses,
    topics
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

run_test(11001621, 11007622, ["0x3883f5e181fccaF8410FA61e12b59BAd963fb645"], [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]]);
run_test(0, 500, ["0x3883f5e181fccaF8410FA61e12b59BAd963fb645"], [["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]]);

run_benchmark(0, 17123123, ["0x7a63d17F5a59BCA04B6702F461b1f1A1c59b100B", "0x282BDD42f4eb70e7A9D9F40c8fEA0825B7f68C5D"], []);
