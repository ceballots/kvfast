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

    pub fn to_bytes(&self) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[0..4].copy_from_slice(&self.magic);
        bytes[4..8].copy_from_slice(&self.version.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.num_keys.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.key_size.to_le_bytes());
        bytes[24..32].copy_from_slice(&(self.values_start as u64).to_le_bytes());
        bytes
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct IndexHeader {
    pub magic: [u8; 4],        // "KIDX"
    pub version: u32,          // Version number
    pub num_keys: u64,         // Number of keys
    pub mphf_size: u64,        // Size of serialized MPHF
    pub keys_offset: u64,      // Offset to keys section
    pub offsets_offset: u64,   // Offset to offsets section
}

impl IndexHeader {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 36 {  // 4 + 4 + 8 + 8 + 8 + 8 = 40 bytes
            return None;
        }

        let magic = bytes[0..4].try_into().ok()?;
        let version = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
        let num_keys = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let mphf_size = u64::from_le_bytes(bytes[16..24].try_into().ok()?);
        let keys_offset = u64::from_le_bytes(bytes[24..32].try_into().ok()?);
        let offsets_offset = u64::from_le_bytes(bytes[32..40].try_into().ok()?);

        if &magic != b"KIDX" {
            return None;
        }

        Some(Self {
            magic,
            version,
            num_keys,
            mphf_size,
            keys_offset,
            offsets_offset,
        })
    }

    pub fn to_bytes(&self) -> [u8; 40] {
        let mut bytes = [0u8; 40];
        bytes[0..4].copy_from_slice(&self.magic);
        bytes[4..8].copy_from_slice(&self.version.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.num_keys.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.mphf_size.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.keys_offset.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.offsets_offset.to_le_bytes());
        bytes
    }
}
