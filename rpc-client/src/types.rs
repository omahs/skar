use skar_format::{BlockHeader, BlockNumber, Hash, Log, TransactionReceipt};
use std::result::Result as StdResult;

pub enum RpcRequestImpl {
    GetBlockNumber,
    GetBlockByNumber(BlockNumber),
    GetLogs(GetLogs),
    GetTransactionReceipt(BlockNumber, Hash),
}

pub enum RpcResponseImpl {
    GetBlockNumber(BlockNumber),
    GetLogs(Vec<Log>),
    GetBlockByNumber(BlockHeader),
    GetTransactionReceipt(TransactionReceipt),
}

pub enum MaybeBatch<T> {
    Single(T),
    Batch(Vec<T>),
}

pub type RpcRequest = MaybeBatch<RpcRequestImpl>;
pub type RpcResponse = MaybeBatch<RpcResponseImpl>;

pub struct GetBlockNumber;

pub struct GetLogs {
    pub from_block: BlockNumber,
    pub to_block: BlockNumber,
}

impl From<GetBlockNumber> for RpcRequest {
    fn from(_: GetBlockNumber) -> Self {
        Self::Single(RpcRequestImpl::GetBlockNumber)
    }
}

impl TryInto<BlockNumber> for RpcResponse {
    type Error = ();

    fn try_into(self) -> StdResult<BlockNumber, Self::Error> {
        match self {
            RpcResponse::Single(RpcResponseImpl::GetBlockNumber(block_num)) => Ok(block_num),
            _ => Err(()),
        }
    }
}

impl From<GetLogs> for RpcRequest {
    fn from(req: GetLogs) -> Self {
        Self::Single(RpcRequestImpl::GetLogs(req))
    }
}

impl TryInto<Vec<Log>> for RpcResponse {
    type Error = ();

    fn try_into(self) -> StdResult<Vec<Log>, Self::Error> {
        match self {
            RpcResponse::Single(RpcResponseImpl::GetLogs(logs)) => Ok(logs),
            _ => Err(()),
        }
    }
}

impl From<&RpcRequest> for serde_json::Value {
    fn from(req: &RpcRequest) -> serde_json::Value {
        match req {
            RpcRequest::Single(req) => req.to_json(0),
            RpcRequest::Batch(reqs) => {
                let arr = reqs
                    .iter()
                    .enumerate()
                    .map(|(idx, req)| req.to_json(idx))
                    .collect::<Vec<_>>();

                serde_json::Value::Array(arr)
            }
        }
    }
}

impl RpcRequestImpl {
    fn to_json(&self, idx: usize) -> serde_json::Value {
        match self {
            RpcRequestImpl::GetBlockNumber => serde_json::json!({
                "method": "eth_blockNumber",
                "params": [],
                "id": idx,
                "jsonrpc": "2.0",
            }),
            RpcRequestImpl::GetBlockByNumber(block_number) => serde_json::json!({
                "method": "eth_getBlockByNumber",
                "params": [
                    block_number,
                    false,
                ],
                "id": idx,
                "jsonrpc": "2.0",
            }),
            RpcRequestImpl::GetLogs(GetLogs {
                from_block,
                to_block,
            }) => serde_json::json!({
                "method": "eth_getBlockByNumber",
                "params": [{
                    "fromBlock": from_block,
                    "toBlock": to_block,
                }],
                "id": idx,
                "jsonrpc": "2.0",
            }),
            RpcRequestImpl::GetTransactionReceipt(_, hash) => serde_json::json!({
                "method": "eth_getTransactionReceipt",
                "params": [hash],
                "id": idx,
                "jsonrpc": "2.0",
            }),
        }
    }
}

impl RpcRequest {
    pub(crate) fn resp_from_json(&self, json: &str) -> Option<RpcResponse> {
        let json = serde_json::from_str(json).ok()?;

        match (self, json) {
            (Self::Batch(reqs), serde_json::Value::Array(arr)) => {
                let mut vals = Vec::new();

                for (idx, (val, req)) in arr.into_iter().zip(reqs.iter()).enumerate() {
                    match val {
                        serde_json::Value::Object(obj) => {
                            vals.push(req.resp_from_json(idx, obj)?);
                        }
                        _ => return None,
                    }
                }

                Some(RpcResponse::Batch(vals))
            }
            (Self::Single(req), serde_json::Value::Object(obj)) => {
                Some(RpcResponse::Single(req.resp_from_json(0, obj)?))
            }
            _ => None,
        }
    }
}

impl RpcRequestImpl {
    fn resp_from_json(&self, idx: usize, mut json: JsonObject) -> Option<RpcResponseImpl> {
        if json.remove("jsonrpc")?.as_str()? != "2.0" {
            return None;
        }

        if json.remove("id")?.as_u64()? != u64::try_from(idx).unwrap() {
            return None;
        }

        let res = json.remove("result")?;

        match self {
            Self::GetBlockNumber => Some(RpcResponseImpl::GetBlockNumber(
                serde_json::from_value(res).ok()?,
            )),
            Self::GetBlockByNumber(_) => serde_json::from_value(res)
                .ok()
                .map(RpcResponseImpl::GetBlockByNumber),
            Self::GetLogs(_) => serde_json::from_value(res)
                .ok()
                .map(RpcResponseImpl::GetLogs),
            Self::GetTransactionReceipt(_, _) => serde_json::from_value(res)
                .ok()
                .map(RpcResponseImpl::GetTransactionReceipt),
        }
    }
}

type JsonObject = serde_json::Map<String, serde_json::Value>;
