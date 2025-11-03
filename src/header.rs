use serde::{Deserialize, Serialize};

#[repr(C)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
// Database Array Based Archive
pub struct DabaHeader {
    pub magic: [u8; 4],      // 4 bytes
    pub version: u32,        // 4 bytes
    pub num_keys: u64,       // 8 bytes
    pub key_size: u64,       // 8 bytes
    pub values_start: usize, // 8 bytes
}

impl DabaHeader {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < std::mem::size_of::<Self>() {
            return None;
        }

        let magic = bytes[0..4].try_into().ok()?;
        let version = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
        let num_keys = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let key_size = u64::from_le_bytes(bytes[16..24].try_into().ok()?);
        let values_start = u64::from_le_bytes(bytes[24..32].try_into().ok()?) as usize;
        if &magic != b"DABA" {
            return None;
        }
        Some(Self {
            magic,
            version,
            num_keys,
            key_size,
            values_start,
        })
    }
}
