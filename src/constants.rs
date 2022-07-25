pub const CHECKSUM_SIZE_IN_BYTES: u64 = 4;
pub const KEY_LEN_SIZE_IN_BYTES: u64 = 4;
pub const VALUE_LEN_SIZE_IN_BYTES: u64 = 4;

/// When an entry is deleted, the value len will be -1.
pub const TOMBSTONE_VALUE_LEN: i32 = -1;
