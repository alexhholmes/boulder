use std::collections::BTreeMap;

pub enum MemTable {
    ReadWrite(MemTableInner),
    ReadOnly(MemTableInner),
    Flushed,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable::ReadOnly(MemTableInner::new())
    }
}

pub struct MemTableInner {
    inner: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Default for MemTableInner {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTableInner {
    pub fn new() -> Self {
        MemTableInner {
            inner: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        self.inner.insert(key.to_vec(), value.to_vec());
    }
}

impl Drop for MemTableInner {
    fn drop(&mut self) {
        todo!()
    }
}
