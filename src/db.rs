use bytes::Bytes;
use crate::batch::{Batch, BatchType};
use crate::key::Key;

pub struct DB {
    
}

impl DB {
    pub fn open() -> Self {
        unimplemented!()
    }
    
    pub fn apply_batch<const T: BatchType>(&self, batch: Batch<T>) -> anyhow::Result<()> {
        unimplemented!()
    }
    
    pub fn insert(&self, key: Bytes, value: Bytes) -> anyhow::Result<()> {
        let mut batch  = Batch::write();
        batch.insert(key, value);
        self.apply_batch(batch)
    }
}