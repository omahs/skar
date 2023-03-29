use skar_format::{Block, BlockNumber, Hash, Log, TransactionReceipt};
use std::result::Result as StdResult;

#[derive(Clone)]
pub enum RpcRequestImpl {
    GetBlockNumber,
    GetBlockByNumber(BlockNumber),
    GetLogs(GetLogs),
    GetTransactionReceipt(BlockNumber, Hash),
}

pub enum RpcResponseImpl {
    GetBlockNumber(BlockNumber),
    GetBlockByNumber(Block<Hash>),
    GetLogs(Vec<Log>),
    GetTransactionReceipt(TransactionReceipt),
}

pub enum MaybeBatch<T> {
    Single(T),
    Batch(Vec<T>),
}

pub type RpcRequest = MaybeBatch<RpcRequestImpl>;
pub type RpcResponse = MaybeBatch<RpcResponseImpl>;

pub struct GetBlockNumber;

#[derive(Clone)]
pub struct GetLogs {
    pub from_block: BlockNumber,
    pub to_block: BlockNumber,
}

impl From<GetBlockNumber> for RpcRequest {
    fn from(_: GetBlockNumber) -> Self {
        Self::Single(RpcRequestImpl::GetBlockNumber)
    }
}

impl TryInto<BlockNumber> for RpcResponseImpl {
    type Error = ();

    fn try_into(self) -> StdResult<BlockNumber, Self::Error> {
        match self {
            RpcResponseImpl::GetBlockNumber(block_num) => Ok(block_num),
            _ => Err(()),
        }
    }
}

impl From<GetLogs> for RpcRequest {
    fn from(req: GetLogs) -> Self {
        Self::Single(RpcRequestImpl::GetLogs(req))
    }
}

impl TryInto<Vec<Log>> for RpcResponseImpl {
    type Error = ();

    fn try_into(self) -> StdResult<Vec<Log>, Self::Error> {
        match self {
            RpcResponseImpl::GetLogs(logs) => Ok(logs),
            _ => Err(()),
        }
    }
}

impl TryInto<Block<Hash>> for RpcResponseImpl {
    type Error = ();

    fn try_into(self) -> StdResult<Block<Hash>, Self::Error> {
        match self {
            RpcResponseImpl::GetBlockByNumber(blocks) => Ok(blocks),
            _ => Err(()),
        }
    }
}

impl<T> TryInto<Vec<T>> for RpcResponse
where
    RpcResponseImpl: TryInto<T, Error = ()>,
{
    type Error = ();

    fn try_into(self) -> StdResult<Vec<T>, Self::Error> {
        match self {
            Self::Batch(resps) => resps.into_iter().map(TryInto::try_into).collect(),
            _ => Err(()),
        }
    }
}

impl TryInto<TransactionReceipt> for RpcResponseImpl {
    type Error = ();

    fn try_into(self) -> StdResult<TransactionReceipt, Self::Error> {
        match self {
            RpcResponseImpl::GetTransactionReceipt(receipt) => Ok(receipt),
            _ => Err(()),
        }
    }
}

impl RpcResponse {
    pub fn try_into_single<T>(self) -> Option<T>
    where
        RpcResponseImpl: TryInto<T, Error = ()>,
    {
        match self {
            Self::Single(v) => v.try_into().ok(),
            _ => None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    fn read_json_file(name: &str) -> String {
        std::fs::read_to_string(format!("{}/test-data/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap()
    }

    #[test]
    fn test_get_block_number() {
        let req = RpcRequest::Single(RpcRequestImpl::GetBlockNumber);

        let _: BlockNumber = req
            .resp_from_json(&read_json_file("eth_blockNumber.json"))
            .unwrap()
            .try_into_single()
            .unwrap();
    }

    #[test]
    fn test_get_block_by_number() {
        let req = RpcRequest::Batch(vec![
            RpcRequestImpl::GetBlockByNumber(13.into()),
            RpcRequestImpl::GetBlockByNumber(14.into()),
            RpcRequestImpl::GetBlockByNumber(15.into()),
        ]);
        let _: Vec<Block<Hash>> = req
            .resp_from_json(&read_json_file("eth_getBlockByNumber_batch.json"))
            .unwrap()
            .try_into()
            .unwrap();
    }

    #[test]
    fn test_get_logs() {
        let req = RpcRequest::Single(RpcRequestImpl::GetLogs(GetLogs {
            from_block: 0.into(),
            to_block: 13.into(),
        }));

        let _: Vec<Log> = req
            .resp_from_json(&read_json_file("eth_getLogs.json"))
            .unwrap()
            .try_into_single()
            .unwrap();
    }

    #[test]
    fn test_get_transaction_receipt() {
        let req = RpcRequest::Batch(vec![
            RpcRequestImpl::GetTransactionReceipt(
                16929247.into(),
                hex!("017e8ad62f871604544a2ac9ea80ce920a0c79c30f11440a7b481ece7f18b2b0").into(),
            ),
            RpcRequestImpl::GetTransactionReceipt(
                16929247.into(),
                hex!("eab31339e74d34155f8b0a92f384672c7b861c07939f7d58d921d5b50fde640e").into(),
            ),
        ]);
        let _: Vec<TransactionReceipt> = req
            .resp_from_json(&read_json_file("eth_getTransactionReceipt_batch.json"))
            .unwrap()
            .try_into()
            .unwrap();
    }
}
