# skar
[![CI](https://github.com/ozgrakkurt/skar/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/ozgrakkurt/skar/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/ozgrakkurt/skar/branch/main/graph/badge.svg?token=SPS7FB1V48)](https://codecov.io/gh/ozgrakkurt/skar)

_**skar**_ is an indexer for Ethereum(EVM) data. It indexes the data and exposes a high-feature API.

## Goals

- Low system requirements compared to indexing the data on a traditional database. Less than 700GB for block headers, transactions, receipts and logs of ethereum mainnet.
- High query performance that is based on efficient indexing of the data and usage of Apache Arrow/Parquet.
- High ingestion speed only limited by the source/network.
- ApacheArrow/GRPC based api. This will help with efficiency because it is lighter than json over http (Work In Progress).
- Open source software and easy deployment. Anyone should be able to deploy with ease and preffered configuration.
- Flexible ingestion from multiple sources with different limiting configurations. This will allow someone with a bunch of limited keys from RPC providers
to combine those keys (and maybe even some free endpoints) to have a premium and zero downtime experience (Work In Progress). This is WIP because currently skar requires the `eth_getBlockReceipts` method to be available on the source, many RPC providers and node implementations don't implement this.
- Index multiple chains with a single _**skar**_ instance. This will allow users to index many chains without running and maintaining many instances of _**skar**_ (Work In Progress).
 
## Status

This project is currently in experimental stage. There will be minor breaking changes to the data format and the API.

## Usage

Currently there is a free to use deployment of skar for ethereum mainnet. It can be reached on `http://91.216.245.118:1151/query`.

If you have rust toolchain installed, you can install skar to path with:
```bash
make install
```

You can directly start skar in current directory for development like so (Also need to have `skar.toml` in project root. See example configuration section for config format):
```bash
make run
```

Then skar can be started like so:
```bash
skar --config-path /path/to/config/file
```
Skar is recommended to be installed and run as a service. This can be done via `systemd` or `supervisord` on linux systems.

#### Example Configuration File (TOML)

```toml
[query]
# Time limit for handling a single query.
#
# If this time limit is hit, the query will stop,
# and the data will be returned to the user.
time_limit_ms = 5000

[http_server]
# Socket address to serve the http server from
addr = "127.0.0.1:1131"
# Response size limit for the http requests.
#
# If reponse payload reaches this size, the query will stop and
# the payload will be returned to client. 
response_size_limit_mb = 30

[db]
# Path to the database directory
path = "data/db"

# Configuration for ingestion of data from ethereum RPC
[ingest]
# This should be zero always, only ingesting from the genesis is supported for now
from_block = 0
# Limit to concurrent http requests
concurrency_limit = 8
# Batch size for Ethereum RPC requests
batch_size = 100

[ingest.rpc_client]
# Timeout for Ethereum RPC requests
http_req_timeout_millis = 5000

# Configuration for an Ethereum RPC Node
# Many endpoints can be added at the same time like this.
#
# Skar will load balance requests, so it will use the other endpoints if the
# limit for an endpoint is reached.
[[ingest.rpc_client.endpoints]]
# Url to the RPC node
url = "https://rpc.ankr.com/eth"
# bearer_token = "my_token" this can be configured if the RPC node requires an Authorization header with bearer token
# The interval for health check of the RPC node
# Skar will ping the RPC node to check the tip block using this interval
status_refresh_interval_secs = 10
# Number of requests that are allowed in a window
req_limit = 10
# The length of the window in milliseconds
req_limit_window_ms = 1000
# Range limit for eth_getLogs requests. eth_getLogs isn't used for now so this is not used.
get_logs_range_limit = 100
# Batch size limit for the requests that are made to this RPC node
batch_size_limit = 100

[parquet]
# path to wirte/read the parquet files
path = "data/parquet"

[parquet.blocks]
# Maximum number of blocks per parquet folder
max_file_size = 200000
# Maximum number of blocks per row group in block parquet files
max_row_group_size = 5000

[parquet.transactions]
max_file_size = 100000
max_row_group_size = 5000

[parquet.logs]
max_file_size = 100000
max_row_group_size = 5000

```

#### Http API

To sync the entire blockchain history with given filter configuration, the client makes consecutive queries using `from_block` to indicate the block to start the query from. The response has a `next_block` field indicating which block to continue the query from.

To sync a particular block range, `from_block` and `to_block` can be used together. If the server can't reach `to_block` in a single request, the client can continue their query using the `next_block` field of the response.

##### Query Fields

- **fromBlock**: Block number to start from (inclusive).
- **toBlock**: Block number to end on (exclusive) (optional). If this is not given, the query will go on for a fixed amount of time or until it reaches the height of the archive.
- **logs.address**: Array of addresses to query for. A log will be included in the response if the log's address matches any of the addresses given in the query. (null or empty array means any address).
- **log.topics**: Array of arrays of topics. Outer array has an element for each topic an EVM log can have. Each inner array represents possible matching values for a topic. For example topics[2] is an array of possible values that should match the log's third topic or the log won't be included in the response. Empty arrays match everything.
- **transactions.from** and **transactions.to**: Array of addresses that should match the transaction's `to` field and the transaction's `from` field. If none of these match, the transaction won't be included in the response. If both are null or empty array, any address will pass.
- **transactions.sighash**: Array of values that should match first four bytes of the transaction input. null or empty array means any value will pass.
- **transactions.status**: Filter by the status of the transaction, only the transactions with this status will be returned. (optional).

##### Example Request

```json
{
  "from_block": 0,
  "to_block": 4728892,
  "logs": [
    {
      "address": [
        "0x3883f5e181fccaF8410FA61e12b59BAd963fb645"
      ],
      "topics": [
        [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        ]
      ]
    }
  ],
  "transactions": [
    {
      "from": [
        "0x3883f5e181fccaF8410FA61e12b59BAd963fb645"
      ],
      "to": [
        "0x3883f5e181fccaF8410FA61e12b59BAd963fb645"
      ],
      "sighash": [],
      "status": 1
    }
  ],
  "field_selection": {
    "block": [
      "number",
      "timestamp",
      "hash"
    ],
    "transaction": [
      "to",
      "value",
      "from"
    ],
    "log": [
      "address",
      "block_number",
      "data",
      "log_index",
      "transaction_hash"
    ]
  }
}
```

##### Example Response

```json
{
  "data": [
    {
      "logs": [
        {
          "log_index": 5,
          "transaction_hash": "0x20f4f97f2766a0ced054c02bc8b539bb779e989d3927746a10e3abbbc351dcc4",
          "block_number": 4728872,
          "address": "0x3883f5e181fccaf8410fa61e12b59bad963fb645",
          "data": "0x000000000000000000000000000000000000000000027b46536c66c8e3000000"
        },
        {
          "log_index": 6,
          "transaction_hash": "0x20f4f97f2766a0ced054c02bc8b539bb779e989d3927746a10e3abbbc351dcc4",
          "block_number": 4728872,
          "address": "0x3883f5e181fccaf8410fa61e12b59bad963fb645",
          "data": "0x00000000000000000000000000000000000000000001a784379d99db42000000"
        },
        {
          "log_index": 39,
          "transaction_hash": "0xe1fc059c4ce0082a7fbd8dbb1d3253d19d29d419300539001d4833cfd54260dc",
          "block_number": 4728890,
          "address": "0x3883f5e181fccaf8410fa61e12b59bad963fb645",
          "data": "0x0000000000000000000000000000000000000000000211654585005212800000"
        }
      ],
      "transactions": [
        {
          "from": "0xbfe0bd83cad590a29ab618e94c2d2757b9010c6b",
          "to": "0xf869e807a6a6f5bacfb0ab21d167e2b41a96be04",
          "value": "0x00"
        },
        {
          "from": "0x03e130eafab61ca4d31923b4043db497a830d2bd",
          "to": "0x3883f5e181fccaf8410fa61e12b59bad963fb645",
          "value": "0x00"
        }
      ],
      "blocks": [
        {
          "number": 4728872,
          "hash": "0x57f4eb05be9cc8a6706be6e6cf3004a8ce206e7f8e6f2c4b3626726c4d6ca5b3",
          "timestamp": "0x5a31e815"
        },
        {
          "number": 4728890,
          "hash": "0x04f28835c4107ebec777d57f7e06fead4d97fe70148b354ad70189c7f40f88b5",
          "timestamp": "0x5a31e918"
        }
      ]
    }
  ],
  "archive_height": 17004299,
  "next_block": 4728892,
  "total_execution_time": 194
}
```
