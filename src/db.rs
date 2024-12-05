use anyhow::Result;
use bytes::Bytes;

use crate::batch::{Batch, BatchType};
use crate::transaction::Transaction;

pub struct DB {

}

impl DB {
    pub fn open() -> Self {
        unimplemented!()
    }

    pub fn apply_batch<const T: BatchType>(&self, _batch: Batch<T>) -> Result<()> {
        unimplemented!()
    }
    
    pub fn transaction(&self) -> TransactionHandle {
        unimplemented!()
    }
    
    pub fn get(&self, _key: Bytes) {
        unimplemented!()
    }

    pub fn insert(&self, key: Bytes, value: Bytes) -> Result<()> {
        let mut batch  = Batch::write();
        batch.insert(key, value);
        self.apply_batch(batch)
    }

    pub fn remove(&self, key: Bytes) -> Result<()> {
        let mut batch  = Batch::write();
        batch.remove(key);
        self.apply_batch(batch)
    }
}