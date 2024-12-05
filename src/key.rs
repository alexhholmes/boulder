use bytes::Bytes;
use std::cmp::Ordering;
use std::fmt::Debug;

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum KeyKind {
    Delete = 0,
    Set = 1,
}

impl TryFrom<u8> for KeyKind {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(KeyKind::Delete),
            1 => Ok(KeyKind::Set),
            _ => Err("Invalid key kind"),
        }
    }
}

/// This is the database's monotonically increasing sequence number when a key write occurs. This
/// differentiates duplicate keys, and it is used in the MVCC implementation.
pub type KeyTimestamp = u64;

pub const TIMESTAMP_RANGE_BEGIN: KeyTimestamp = 0;
pub const TIMESTAMP_RANGE_END: KeyTimestamp = u64::MAX >> 8;

#[derive(Copy, Clone, Eq, PartialEq)]
/// The KeyTrailer starts with a 56-bit sequence number followed by a 7-bit key kind.
pub struct KeyTrailer(u64);

impl KeyTrailer {
    pub fn new(ts: KeyTimestamp, kind: KeyKind) -> Self {
        KeyTrailer(ts << 8 | kind as u64)
    }

    fn kind(&self) -> KeyKind {
        KeyKind::try_from((self.0 & 0xff) as u8).unwrap()
    }

    fn timestamp(&self) -> KeyTimestamp {
        self.0 >> 8
    }
}

impl Into<KeyKind> for KeyTrailer {
    fn into(self) -> KeyKind {
        self.kind()
    }
}

impl Into<KeyTimestamp> for KeyTrailer {
    fn into(self) -> KeyTimestamp {
        self.timestamp()
    }
}

pub struct Key<T: AsRef<[u8]>>(T, KeyTrailer);

pub type KeySlice<'a> = Key<&'a [u8]>;
pub type KeyVec = Key<Vec<u8>>;
pub type KeyBytes = Key<Bytes>;

impl<T: AsRef<[u8]>> Key<T> {
    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn trailer(&self) -> KeyTrailer {
        self.1
    }

    pub fn timestamp(&self) -> KeyTimestamp {
        self.1.into()
    }

    pub fn kind(&self) -> KeyKind {
        self.1.into()
    }

    pub fn key_len(&self) -> usize {
        self.0.as_ref().len()
    }

    pub fn raw_len(&self) -> usize {
        self.0.as_ref().len() + size_of::<u64>()
    }

    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_empty()
    }
}

impl Key<Vec<u8>> {
    pub fn clear(&mut self) {
        self.0.clear()
    }

    pub fn extend(&mut self, data: &[u8]) {
        self.0.extend(data)
    }

    pub fn as_key_slice(&self) -> KeySlice {
        Key(self.0.as_slice(), self.1)
    }

    pub fn into_key_bytes(self) -> KeyBytes {
        Key(self.0.into(), self.1)
    }

    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Key<Bytes> {
    pub fn new() -> Self {
        Self(Bytes::new(), KeyTrailer::new(TIMESTAMP_RANGE_BEGIN, KeyKind::Delete))
    }

    pub fn as_key_slice(&self) -> KeySlice {
        Key(&self.0, self.1)
    }

    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<'a> Key<&'a [u8]> {
    pub fn to_key_vec(self) -> KeyVec {
        Key(self.0.to_vec(), self.1)
    }

    pub fn key_ref(self) -> &'a [u8] {
        self.0
    }
}

impl<T: AsRef<[u8]> + Debug> Debug for Key<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: AsRef<[u8]> + PartialEq> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        (self.0.as_ref(), self.1.timestamp() as u64).eq(&(other.0.as_ref(), other.1.timestamp() as u64))
    }
}

impl<T: AsRef<[u8]> + Eq> Eq for Key<T> {}

impl<T: AsRef<[u8]> + Clone> Clone for Key<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1)
    }
}

impl<T: AsRef<[u8]> + Copy> Copy for Key<T> {}

impl<T: AsRef<[u8]> + PartialOrd> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.0.as_ref(), self.1.timestamp() as u64).partial_cmp(&(other.0.as_ref(), other.1.timestamp() as u64))
    }
}

impl<T: AsRef<[u8]> + Ord> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.0.as_ref(), self.1.timestamp() as u64).cmp(&(other.0.as_ref(), other.1.timestamp() as u64))
    }
}
