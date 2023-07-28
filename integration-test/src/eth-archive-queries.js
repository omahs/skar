const getLogsRequestBody = (fromBlock, toBlock, addresses, topics) => {
  return {
    fromBlock,
    toBlock,
    logs: [
      {
        address: addresses, //["0x3883f5e181fccaF8410FA61e12b59BAd963fb645"],
        topics,
        fieldSelection: {
          block: {
            parentHash: true,
            sha3Uncles: true,
            miner: true,
            stateRoot: true,
            transactionsRoot: true,
            receiptsRoot: true,
            logsBloom: true,
            difficulty: true,
            number: true,
            gasLimit: true,
            gasUsed: true,
            timestamp: true,
            extraData: true,
            mixHash: true,
            nonce: true,
            totalDifficulty: true,
            baseFeePerGas: true,
            size: true,
            hash: true,
          },
          transaction: {
            type: true,
            nonce: true,
            to: true,
            gas: true,
            value: true,
            input: true,
            maxPriorityFeePerGas: true,
            maxFeePerGas: true,
            yParity: true,
            chainId: true,
            v: true,
            r: true,
            s: true,
            from: true,
            blockHash: true,
            blockNumber: true,
            index: true,
            gasPrice: true,
            hash: true,
            status: true,
          },
          log: {
            address: true,
            blockHash: true,
            blockNumber: true,
            data: true,
            index: true,
            removed: true,
            topics: true,
            transactionHash: true,
            transactionIndex: true,
          },
        },
      },
    ],
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

const getDataFromEthArchive = async (makeRequestBody, fromBlock, toBlock) => {
  const endPoint = "https://eth.archive.subsquid.io/query";

  let data = [];
  let total = 0;
  let nextBlock = fromBlock;

  console.log(`Syncing using eth-archive from ${fromBlock} to ${toBlock}`);

  while (true) {
    const req = makeRequestBody(nextBlock, toBlock);
    const respBody = await getResp(endPoint, req);
    const { totalTime } = respBody;
    nextBlock = respBody.nextBlock;
    data = [...data, ...respBody.data];
    console.log(
      `scanned ${nextBlock - req.fromBlock} blocks in ${totalTime} ms`
    );
    console.log(`nextBlock: ${nextBlock}`);
    total += totalTime;
    if (toBlock <= nextBlock) {
      break;
    }
    req.fromBlock = nextBlock;
  }

  console.log(`Finished sync in ${total} milliseconds`);

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

module.exports.getDataFromEthArchive = getDataFromEthArchive;
module.exports.getLogsRequestBody = getLogsRequestBody;
