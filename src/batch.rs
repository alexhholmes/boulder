use std::marker::ConstParamTy;
use std::collections::BTreeMap;
use bytes::Bytes;

#[derive(Clone, ConstParamTy, Debug, Eq, PartialEq)]
pub enum BatchType {
    Read,
    Write,
}

/// A batch of updates that are applied atomically to the database. A batch is
/// either a `Read` or a `Write`. `Write` batches will mutate the database.
/// Recurrent keys will overwrite previous writes and result in a single
/// returned item for reads.
///
/// # Examples
/// ```
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     use crate::{Batch, DB};
///
///     let db = DB::open("batch_db")?;
///
///     let mut batch = Batch::read();
///     batch.read("key_0");
///     batch.read("key_1");
///     batch.read("key_2");
///     batch.read("key_3");
///     db.apply_batch(batch)?;
///
///     let mut batch = Batch::write();
///     batch.insert("key_0", "val_0");
///     batch.insert("key_1", "val_1");
///     batch.remove("key_0");
///     batch.insert("key_2", "val_2");
///     db.apply_batch(batch)?;
///
///     Ok(())
/// }
/// ```
pub struct Batch<const T: BatchType> {
    pub(crate) items: BTreeMap<Bytes, Option<Bytes>>,
}

impl Batch<{ BatchType::Read }> {
    pub fn read() -> Batch<{ BatchType::Read }> {
        Batch {
            items: BTreeMap::new(),
        }
    }
    
    pub fn get<K>(&mut self, key: K)
    where
        K: AsRef<[u8]>,
    {
        self.items.insert(Bytes::copy_from_slice(key.as_ref()), None);
    }
}

impl Batch<{ BatchType::Write }> {
    pub fn write() -> Batch<{ BatchType::Write }> {
        Batch {
            items: BTreeMap::new(),
        }
    }
    
    pub fn insert<K, V>(&mut self, key: K, value: V)
    where
        K: Into<Bytes>,
        V: Into<Bytes>,
    {
        self.items.insert(key.into(), Some(value.into()));
    }
    
    pub fn remove<K>(&mut self, key: K)
    where
        K: Into<Bytes>,
    {
        self.items.insert(key.into(), None);
    }
}
