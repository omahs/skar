use serde::{Deserialize, Serialize};
use skar_ingest::IngestConfig;

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub ingest: IngestConfig,
}
