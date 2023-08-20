use std::collections::BTreeSet;

use skar_client::{Client, Config};
use skar_net_types::{FieldSelection, Query};

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_api() {
    let client = Client::new(Config {
        url: "http://91.216.245.118:1151/query/arrow-ipc"
            .parse()
            .unwrap(),
        bearer_token: None,
        http_req_timeout_millis: 20000.try_into().unwrap(),
    });

    let mut block_field_selection = BTreeSet::new();
    block_field_selection.insert("number".to_owned());
    block_field_selection.insert("timestamp".to_owned());
    block_field_selection.insert("hash".to_owned());

    let res = client
        .send(Query {
            from_block: 14000000,
            to_block: None,
            logs: Vec::new(),
            transactions: Vec::new(),
            include_all_blocks: true,
            field_selection: FieldSelection {
                block: block_field_selection,
                log: Default::default(),
                transaction: Default::default(),
            },
        })
        .await
        .unwrap();

    dbg!(res.next_block);
}
