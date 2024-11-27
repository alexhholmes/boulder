mod tree;

use std::ops::Deref;
use std::sync::Arc;
use crate::tree::{MemTable, MemTableInner};

pub struct DB {
    tree: Arc<MemTable>,
}

impl DB {
    pub fn open() -> Self {
        DB {
            tree: Arc::new(MemTable::new())
        }
    }
}

impl Deref for DB {
    type Target = MemTableInner;

    fn deref(&self) -> &Self::Target {
        &self.tree
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        todo!()
    }
}
