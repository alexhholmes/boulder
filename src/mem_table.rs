use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crate::key::{KeyBytes, KeySlice};

struct MemoryTable {
    id: usize,
    approximate_size: Arc<AtomicUsize>,
    list: Arc<SkipMap<KeyBytes, Bytes>>,
}

impl MemoryTable {
    pub fn new(id: usize) -> Self {
        MemoryTable {
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
            list: Arc::new(SkipMap::new()),
        }
    }

    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        self.list
            .get(Bytes::from_static())
            .and_then(|e| Some(e.value().to_owned()))
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.list.insert(key.into(), value.into());
        Ok(())
    }

    pub fn delete(&self, key: KeySlice) -> Result<()> {
        self.list.insert(key.into(), Bytes::new());
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    /// Returns the approximate size of the memtable.
    pub fn size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }
}

impl Drop for MemoryTable {
    fn drop(&mut self) {
        unimplemented!()
    }
}
