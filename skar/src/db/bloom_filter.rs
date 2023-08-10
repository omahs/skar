use std::fmt;

use anyhow::Context;
use sbbf_rs_safe::Filter as Sbbf;
use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug)]
pub struct BloomFilter(pub Sbbf);

impl BloomFilter {}

impl Serialize for BloomFilter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.0.as_bytes())
    }
}

struct BloomFilterVisitor;

impl<'de> Visitor<'de> for BloomFilterVisitor {
    type Value = BloomFilter;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("bloom filter bytes")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Sbbf::from_bytes(v)
            .context("parse bloom filter")
            .map(BloomFilter)
            .map_err(|e| E::custom(e.to_string()))
    }
}

impl<'de> Deserialize<'de> for BloomFilter {
    fn deserialize<D>(deserializer: D) -> Result<BloomFilter, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(BloomFilterVisitor)
    }
}
