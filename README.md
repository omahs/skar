# skar
[![CI](https://github.com/ozgrakkurt/skar/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/ozgrakkurt/skar/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/ozgrakkurt/skar/branch/main/graph/badge.svg?token=SPS7FB1V48)](https://codecov.io/gh/ozgrakkurt/skar)

_**skar**_ is an accelerator for Ethereum(EVM) data. It indexes the data and exposes a low-level, high-feature API.
This API is intended to be directly used by users or by integrations with other tools like TheGraph indexer,
 Subsquid SDK, Ethereum RPC API providers.

## Goals

- Low system requirements compared to indexing the data on a traditional database.
- High query performance that is based on efficient indexing of the data and usage of Apache Arrow/Parquet.
- High ingestion speed only limited by the source/network.
- Expose an additional API that uses Apache Arrow Flight format instead of JSON over HTTP, to reduce network usage.
- Develop a fully featured client library to be used in integrations with other systems and by users.
- Publish pre-built binaries and docker images that make running on user machine or cloud super easy.
- Flexible ingestion from multiple sources with different limiting configurations. This will allow someone with a bunch of limited keys from RPC providers
to combine those keys (and maybe even some free endpoints) to have a premium and zero downtime experience.
- Index multiple chains with a single _**skar**_ instance. This will allow users to index many chains without running and maintaining many instances of _**skar**_.
 
## Status

This project is currently in early stage of development and not ready to be used.

## Example Config

```toml
[http_server]
addr = "127.0.0.1:1131"
response_size_limit_mb = 30
response_time_limit_ms = 5000

[db]
path = "data/db"

[ingest]
from_block = 0
to_block = 18000000
concurrency_limit = 8
batch_size = 100

[parquet]
path = "data/parquet"

[parquet.blocks]
max_file_size = 100000
max_row_group_size = 10000
data_page_size_limit = 1024000

[parquet.transactions]
max_file_size = 100000
max_row_group_size = 10000
data_page_size_limit = 1000000

[parquet.logs]
max_file_size = 100000
max_row_group_size = 10000
data_page_size_limit = 1000000

[ingest.rpc_client]
http_req_timeout_millis = 5000

[[ingest.rpc_client.endpoints]]
url = "https://rpc.ankr.com/eth"
status_refresh_interval_secs = 10
req_limit = 10
req_limit_window_ms = 1000
get_logs_range_limit = 100
batch_size_limit = 100
```
