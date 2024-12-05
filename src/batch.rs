use std::marker::ConstParamTy;
use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

#[derive(ConstParamTy, Eq, PartialEq)]
pub enum BatchType {
    Read,
    Write,
}

pub struct Batch<const T: BatchType> {
    items: BTreeMap<Bytes, Option<Bytes>>,
}

impl Batch<{ BatchType::Read }> {
    pub fn get<K>(&mut self, key: K)
    where
        K: AsRef<[u8]>,
    {
        self.items.insert(key.into(), None);
    }
}

impl Batch<{ BatchType::Write }> {
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