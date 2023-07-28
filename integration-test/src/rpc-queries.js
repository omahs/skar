const RPC_URL = "https://rpc.ankr.com/eth";
const MAX_QUERY_CHUNK_SIZE = 100;
const MAX_BLOCK_RANGE = 1000;

//Used to create the correct format of hex from a number
//that is compatable with JSON RPC.
//NOTE: cannot use ethers.toBeHex since it pads zeros on
//the hex. ie. we need "0x1" and not "0x01"
const toHex = (num) => "0x" + num.toString(16);

//Uses the eth_getLogs JSON rpc method and chunks queries to retrieve
//all logs with the given filter paramaters
const getLogs = async (fromBlock, toBlock, addresses, topics) => {
  //Executes the rpc queury
  const executeChunk = async (chunkFrom, chunkTo) => {
    const params = [
      {
        fromBlock: toHex(chunkFrom),
        toBlock: toHex(chunkTo),
        address: addresses,
        topics,
      },
    ];

    const request = {
      jsonrpc: "2.0",
      method: "eth_getLogs",
      params,
      id: 1,
    };

    const body = JSON.stringify(request);
    const requestOptions = {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body,
    };
    let res = await fetch(RPC_URL, requestOptions);
    let data = await res.json();
    return data.result;
  };

  //Chunk the the block range so as to not hit rpc range limits
  let chunkFrom = fromBlock;
  let chunkTo = Math.min(toBlock, chunkFrom + MAX_BLOCK_RANGE);
  let data = [];
  while (chunkFrom < toBlock) {
    const res = await executeChunk(chunkFrom, chunkTo);
    data = [...data, ...res];
    chunkFrom = chunkTo + 1;
    chunkTo = Math.min(toBlock, chunkFrom + MAX_BLOCK_RANGE);
  }

  return data;
};

//Retrieve all transactions that match the given array
//of tx hashes. The resulting values are merged objects
//containing both the transaction data and the transaction
//receipt data.
const getTransactions = async (txHashes) => {
  const executeChunk = async (chunk) => {
    const geRequestBody = (txHash, method, i) => ({
      jsonrpc: "2.0",
      method,
      params: [txHash],
      id: i + 1,
    });

    //create an array of requests where they are alternating eth_getTransactionByHash
    //and eth_getTransactionReceipt
    let requests = chunk
      .map((txHash, i) => [
        geRequestBody(txHash, "eth_getTransactionByHash", (i + 1) * 2 - 1),
        geRequestBody(txHash, "eth_getTransactionReceipt", (i + 1) * 2),
      ])
      .flat();

    const requestOptions = {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(requests),
    };
    let res = await fetch(RPC_URL, requestOptions);
    let data = await res.json();

    let mergedData = [];
    while (data.length > 0) {
      //splice out the tx and txReceipt value pairs
      //and merge their data
      const spliced = data.splice(0, 2);
      const [tx, txReceipt] = spliced;

      const mergedVal = {
        ...tx.result,
        ...txReceipt.result,
      };

      mergedData.push(mergedVal);
    }

    return mergedData;
  };

  const txHashesCopy = [...txHashes];
  let data = [];
  while (txHashesCopy.length > 0) {
    //Chunk should be half the max query chunk size since it incurs
    //two queries per chunk
    const chunk = txHashesCopy.splice(0, MAX_QUERY_CHUNK_SIZE / 2);

    const res = await executeChunk(chunk);
    data = [...data, ...res];
  }

  return data;
};

//Uses the eth_getBlockByNumber JSON rpc method and chunks queries to retrieve
//all blocknumbers specified in the array of blocknumbers. Pass in boolean
//"withTransactions" to add all the transaction values of the block in the response
const getBlocks = async (blocknumbers, withTransactions) => {
  //Query executor
  const executeChunk = async (chunk) => {
    let requests = chunk.map((blocknumber, i) => ({
      jsonrpc: "2.0",
      method: "eth_getBlockByNumber",
      params: [blocknumber, withTransactions],
      id: i + 1,
    }));

    const requestOptions = {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(requests),
    };

    let res = await fetch(RPC_URL, requestOptions);
    let data = await res.json();

    return data.map((res) => res.result);
  };

  //Chunking rpc queries to stay with in limit of batch requests
  const blocknumberscopy = [...blocknumbers];
  let data = [];
  while (blocknumberscopy.length > 0) {
    const chunk = blocknumberscopy.splice(0, MAX_QUERY_CHUNK_SIZE);

    const res = await executeChunk(chunk);
    data = [...data, ...res];
  }

  return data;
};

//Gets all logs in a log filter and then finds all related
//blocks and transactions to those logs
const getLogsWithTxsAndBlocks = async (
  startBlock,
  endBlock,
  addresses,
  topics
) => {
  let logs = await getLogs(startBlock, endBlock, addresses, topics);

  const blocknumbers = logs.map((log) => log.blockNumber);
  const withTransactions = false;
  const blocks = await getBlocks(blocknumbers, withTransactions);
  const txHashes = logs.map((log) => log.transactionHash);
  const transactions = await getTransactions(txHashes);

  return {
    logs,
    transactions,
    blocks,
  };
};

module.exports.getLogsWithTxsAndBlocks = getLogsWithTxsAndBlocks;
