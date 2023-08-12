const getLogsRequestBody = (from_block, to_block, addresses, topics) => {
  return {
    from_block,
    to_block: to_block,
    logs: [
      {
        address: addresses, //["0x3883f5e181fccaF8410FA61e12b59BAd963fb645"],
        topics,
      },
    ],
    field_selection: {
      block: [
        "sha3_uncles",
        "miner",
        "state_root",
        "transactions_root",
        "receipts_root",
        "logs_bloom",
        "difficulty",
        "number",
        "gas_limit",
        "gas_used",
        "timestamp",
        "extra_data",
        "nonce",
        "total_difficulty",
        "size",
        "hash",
      ],
      transaction: [
        "type",
        "nonce",
        "to",
        "gas",
        "value",
        "input",
        "v",
        "r",
        "s",
        "from",
        "block_hash",
        "block_number",
        "transaction_index",
        "gas_price",
        "hash",
        "status",
      ],
      log: [
        "address",
        "block_hash",
        "block_number",
        "data",
        "log_index",
        "removed",
        "topic0",
        "topic1",
        "topic2",
        "topic3",
        "transaction_hash",
        "transaction_index",
      ],
    },
    // transactions: [
    //   {
    //     address: ["0x3883f5e181fccaf8410fa61e12b59bad963fb645"],
    //     sighash: null, // ["0xa9059cbb"],
    //     fieldSelection: {
    //       block: {
    //         parentHash: true,
    //         sha3Uncles: true,
    //         miner: true,
    //         stateRoot: true,
    //         transactionsRoot: true,
    //         receiptsRoot: true,
    //         logsBloom: true,
    //         difficulty: true,
    //         number: true,
    //         gasLimit: true,
    //         gasUsed: true,
    //         timestamp: true,
    //         extraData: true,
    //         mixHash: true,
    //         nonce: true,
    //         totalDifficulty: true,
    //         baseFeePerGas: true,
    //         size: true,
    //         hash: true,
    //       },
    //       transaction: {
    //         type: true,
    //         nonce: true,
    //         to: true,
    //         gas: true,
    //         value: true,
    //         input: true,
    //         maxPriorityFeePerGas: true,
    //         maxFeePerGas: true,
    //         yParity: true,
    //         chainId: true,
    //         v: true,
    //         r: true,
    //         s: true,
    //         from: true,
    //         blockHash: true,
    //         blockNumber: true,
    //         index: true,
    //         gasPrice: true,
    //         hash: true,
    //         status: true,
    //       },
    //       log: {
    //         address: true,
    //         blockHash: true,
    //         blockNumber: true,
    //         data: true,
    //         index: true,
    //         removed: true,
    //         topics: true,
    //         transactionHash: true,
    //         transactionIndex: true,
    //       },
    //     },
    //   },
    // ],
  };
};

const getDataFromSkar = async (makeRequestBody, fromBlock, toBlockInclusive) => {
  const toBlock = toBlockInclusive+1;
  const endPoint = "http://91.216.245.118:1151/query";

  let data = [];
  let total = 0;
  let nextBlock = fromBlock;

  console.log(`Syncing using skar from ${fromBlock} to ${toBlockInclusive}`);

  while (true) {
    const req = makeRequestBody(nextBlock, toBlock);
    const respBody = await getResp(endPoint, req);
    const totalTime = respBody.total_execution_time;
    nextBlock = respBody.next_block;
    data = [...data, ...respBody.data];
    console.log(
      `scanned ${nextBlock - req.from_block} blocks in ${totalTime} ms`
    );
    console.log(`nextBlock: ${nextBlock}`);
    total += totalTime;
    if (toBlock <= nextBlock) {
      break;
    }
    req.fromBlock = nextBlock;
  }

  console.log(`Finished skar sync in ${total} milliseconds`);

  return data;
};

async function getResp(url, req) {
  while (true) {
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(req),
        signal: AbortSignal.timeout(15000),
      });

      return await resp.json();
    } catch (e) {
      console.error(e);
      console.log(JSON.stringify(req, null, 2));
    }
  }
}

module.exports.getDataFromSkar = getDataFromSkar;
module.exports.getLogsRequestBodySkar = getLogsRequestBody;
