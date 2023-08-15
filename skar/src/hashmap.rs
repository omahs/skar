use std::collections::HashSet as StdHashSet;
use xxhash_rust::xxh3::Xxh3Builder;

pub type FastSet<T> = StdHashSet<T, Xxh3Builder>;
