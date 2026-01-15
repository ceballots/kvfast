use crate::header::{DabaHeader, IndexHeader};
use epserde::prelude::*;
use memmap2::Mmap;
use ptr_hash::{bucket_fn::CubicEps, PtrHash, PtrHashParams};
use std::path::Path;
use std::{
    fs::File,
    io::{self, Seek, SeekFrom, Write},
    sync::Arc,
};

pub const KEY_SIZE: usize = 16;
pub type Key = [u8; KEY_SIZE];

// Type alias for PtrHash with default type parameters
// We only specify Key and BucketFn, letting the other parameters use defaults
pub type KeyPtrHash = PtrHash<Key, CubicEps>;

pub struct Database {
    header: DabaHeader,
    mmap_data: Arc<Mmap>,                   // mmap of data file (values)
    offsets: Arc<Vec<u64>>,                 // offsets array in memory
    keys: Arc<Vec<Key>>,                    // keys array for validation
    mphf: MemCase<<KeyPtrHash as DeserializeInner>::DeserType<'static>>,  // minimal perfect hash of keys with epserde wrapper
}

/* +--------------------+
| key 0 (KEY_SIZE)   |
| offset 0 (u64)     |
| key 1 (KEY_SIZE)   |
| offset 1 (u64)     |
| ...                |
+--------------------+ */

impl Database {
    pub fn open<P: AsRef<Path>>(data_file: P, index_file: P) -> io::Result<Self> {
        // Open and mmap the data file
        let file = File::open(data_file)?;
        let mmap_data = unsafe { Mmap::map(&file)? };

        let header_size = std::mem::size_of::<DabaHeader>();
        let header = DabaHeader::from_bytes(&mmap_data[0..header_size])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid DABA header"))?;

        let num_keys = header.num_keys as usize;

        // Open and mmap the index file
        let idx_file = File::open(index_file)?;
        let index_mmap = unsafe { Mmap::map(&idx_file)? };

        // Parse index header
        let index_header = IndexHeader::from_bytes(&index_mmap[0..40])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid index header"))?;

        if index_header.num_keys != header.num_keys {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Key count mismatch between data and index files",
            ));
        }

        // Deserialize MPHF from index file using epserde
        // We need to create a temporary file because epserde expects a file path
        let mphf_start = 40;
        let mphf_end = mphf_start + index_header.mphf_size as usize;
        let mphf_bytes = &index_mmap[mphf_start..mphf_end];

        // Write MPHF bytes to a temporary file
        let mut temp_mphf_file = tempfile::NamedTempFile::new()?;
        temp_mphf_file.write_all(mphf_bytes)?;
        temp_mphf_file.flush()?;

        // Load MPHF using epserde's mmap-based deserialization
        let mphf = <KeyPtrHash as Deserialize>::mmap(temp_mphf_file.path(), epserde::deser::Flags::empty())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("Failed to deserialize MPHF: {:?}", e)))?;

        // Parse keys from index file
        let keys_start = index_header.keys_offset as usize;
        let mut keys = Vec::with_capacity(num_keys);

        for i in 0..num_keys {
            let offset = keys_start + i * KEY_SIZE;
            let key_bytes: Key = index_mmap[offset..offset + KEY_SIZE]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid key in index"))?;
            keys.push(key_bytes);
        }

        // Parse offsets from index file
        let offsets_start = index_header.offsets_offset as usize;
        let mut offsets = Vec::with_capacity(num_keys);

        for i in 0..num_keys {
            let offset = offsets_start + i * 8;
            let value = u64::from_le_bytes(
                index_mmap[offset..offset + 8]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid offset in index"))?
            );
            offsets.push(value);
        }

        Ok(Self {
            mmap_data: Arc::new(mmap_data),
            offsets: Arc::new(offsets),
            keys: Arc::new(keys),
            mphf,
            header,
        })
    }

    pub fn get(&self, key: &Key) -> Option<&[u8]> {
        // PtrHash uses index() method which returns the hash index
        let idx = self.mphf.index(key) as usize;

        // Validate the key matches to prevent false positives
        if idx >= self.keys.len() || &self.keys[idx] != key {
            return None;
        }

        let start = self.offsets[idx] as usize;
        let end = self
            .offsets
            .get(idx + 1)
            .map(|&v| v as usize)
            .unwrap_or(self.mmap_data.len() - self.header.values_start);

        Some(&self.mmap_data[self.header.values_start + start..self.header.values_start + end])
    }

    pub fn write_database<K, V, PK, PV, P>(
        path_data: P,
        path_index: P,
        keys_iter: K,
        values_iter: V,
        version: u32,
    ) -> io::Result<()>
    where
        K: Iterator<Item = PK>,
        PK: AsRef<[u8]>,
        V: Iterator<Item = PV>,
        PV: AsRef<[u8]>,
        P: AsRef<Path>,
    {
        // TODO: evaluate the use of iterators. The limitation rn is the mphf.
        let mut data_file = File::create(&path_data)?;
        let header_size = std::mem::size_of::<DabaHeader>();
        data_file.write_all(&vec![0u8; header_size])?;

        let mut keys_vec = Vec::new();
        let mut values_vec = Vec::new();

        for (k, v) in keys_iter.zip(values_iter) {
            let key_bytes = k.as_ref();
            assert_eq!(key_bytes.len(), KEY_SIZE, "Key must be {} bytes", KEY_SIZE);

            let mut key: Key = [0u8; KEY_SIZE];
            key.copy_from_slice(key_bytes);
            keys_vec.push(key);
            values_vec.push(v.as_ref().to_vec());
        }

        let num_keys = keys_vec.len() as u64;

        // Build PtrHash with default parameters
        let mphf: KeyPtrHash = PtrHash::new(&keys_vec, PtrHashParams::default());

        // Create mapping from MPHF index to original index
        let mut mphf_to_original = vec![0usize; keys_vec.len()];
        for (original_idx, key) in keys_vec.iter().enumerate() {
            let mphf_idx = mphf.index(key) as usize;
            mphf_to_original[mphf_idx] = original_idx;
        }

        // Write values in the order determined by MPHF
        for mphf_idx in 0..keys_vec.len() {
            let original_idx = mphf_to_original[mphf_idx];
            let val_bytes = &values_vec[original_idx];
            data_file.write_all(val_bytes)?;
        }

        // Serialize MPHF using epserde
        let mut mphf_bytes = Vec::new();
        mphf.serialize(&mut mphf_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("Failed to serialize MPHF: {:?}", e)))?;

        // Calculate offsets for index file sections
        let index_header_size = 40u64;
        let mphf_size = mphf_bytes.len() as u64;
        let keys_offset = index_header_size + mphf_size;
        let offsets_offset = keys_offset + (num_keys * KEY_SIZE as u64);

        // Create index header
        let index_header = IndexHeader {
            magic: *b"KIDX",
            version: 1,
            num_keys,
            mphf_size,
            keys_offset,
            offsets_offset,
        };

        // Write index file
        let mut index_file = File::create(&path_index)?;

        // Write index header
        index_file.write_all(&index_header.to_bytes())?;

        // Write serialized MPHF
        index_file.write_all(&mphf_bytes)?;

        // Write keys in MPHF order
        for mphf_idx in 0..keys_vec.len() {
            let original_idx = mphf_to_original[mphf_idx];
            let key = &keys_vec[original_idx];
            index_file.write_all(key)?;
        }

        // Write offsets in MPHF order
        let mut cursor = 0u64;
        for mphf_idx in 0..keys_vec.len() {
            let original_idx = mphf_to_original[mphf_idx];
            index_file.write_all(&cursor.to_le_bytes())?;
            cursor += values_vec[original_idx].len() as u64;
        }

        // Write data file header
        let header = DabaHeader {
            magic: *b"DABA",
            version,
            num_keys,
            key_size: KEY_SIZE as u64,
            values_start: header_size as usize,
        };
        data_file.seek(SeekFrom::Start(0))?;
        data_file.write_all(&header.to_bytes())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_write_and_read_database() -> io::Result<()> {
        let data_file = NamedTempFile::new()?;
        let index_file = NamedTempFile::new()?;

        let keys: Vec<Key> = vec![
            *b"key0000000000001",
            *b"key0000000000002",
            *b"key0000000000003",
        ];

        let values: Vec<Vec<u8>> = vec![b"hello".to_vec(), b"world".to_vec(), b"rustlang".to_vec()];

        Database::write_database(
            data_file.path(),
            index_file.path(),
            keys.iter(), // &Key
            values.iter().map(|v| v.as_slice()),
            1,
        )?;

        let db = Database::open(data_file.path(), index_file.path())?;

        for (key, value) in keys.iter().zip(values.iter()) {
            let retrieved = db.get(key).expect("Value should exist");
            assert_eq!(retrieved, value.as_slice());
        }

        let missing_key: Key = *b"missing000000001";
        assert!(db.get(&missing_key).is_none());

        Ok(())
    }

    #[test]
    fn test_header_serialization_round_trip() {
        let header = DabaHeader {
            magic: *b"DABA",
            version: 1,
            num_keys: 12345,
            key_size: KEY_SIZE as u64,
            values_start: 32,
        };

        let bytes = header.to_bytes();
        let parsed = DabaHeader::from_bytes(&bytes).expect("Should parse header");

        assert_eq!(header.magic, parsed.magic);
        assert_eq!(header.version, parsed.version);
        assert_eq!(header.num_keys, parsed.num_keys);
        assert_eq!(header.key_size, parsed.key_size);
        assert_eq!(header.values_start, parsed.values_start);
    }

    #[test]
    fn test_key_validation() -> io::Result<()> {
        let data_file = NamedTempFile::new()?;
        let index_file = NamedTempFile::new()?;

        let keys: Vec<Key> = vec![
            *b"validkey00000001",
            *b"validkey00000002",
        ];

        let values: Vec<Vec<u8>> = vec![b"value1".to_vec(), b"value2".to_vec()];

        Database::write_database(
            data_file.path(),
            index_file.path(),
            keys.iter(),
            values.iter().map(|v| v.as_slice()),
            1,
        )?;

        let db = Database::open(data_file.path(), index_file.path())?;

        // Test that valid keys return their values
        for (key, value) in keys.iter().zip(values.iter()) {
            let retrieved = db.get(key).expect("Valid key should return value");
            assert_eq!(retrieved, value.as_slice());
        }

        // Test that invalid keys return None (not garbage data)
        let invalid_key: Key = *b"invalidkey000001";
        assert!(
            db.get(&invalid_key).is_none(),
            "Invalid key should return None, not garbage data"
        );

        Ok(())
    }

    #[test]
    fn test_last_value_retrieval() -> io::Result<()> {
        // Test with single key-value pair
        let data_file = NamedTempFile::new()?;
        let index_file = NamedTempFile::new()?;

        let keys: Vec<Key> = vec![*b"onlykey000000001"];
        let values: Vec<Vec<u8>> = vec![b"single_value".to_vec()];

        Database::write_database(
            data_file.path(),
            index_file.path(),
            keys.iter(),
            values.iter().map(|v| v.as_slice()),
            1,
        )?;

        let db = Database::open(data_file.path(), index_file.path())?;
        let retrieved = db.get(&keys[0]).expect("Should retrieve single value");
        assert_eq!(retrieved, b"single_value");

        // Test with multiple keys where last value is important
        let data_file2 = NamedTempFile::new()?;
        let index_file2 = NamedTempFile::new()?;

        let keys2: Vec<Key> = vec![
            *b"key0000000000001",
            *b"key0000000000002",
            *b"lastkey000000003",
        ];
        let values2: Vec<Vec<u8>> = vec![
            b"first".to_vec(),
            b"middle".to_vec(),
            b"last_value_here".to_vec(),
        ];

        Database::write_database(
            data_file2.path(),
            index_file2.path(),
            keys2.iter(),
            values2.iter().map(|v| v.as_slice()),
            1,
        )?;

        let db2 = Database::open(data_file2.path(), index_file2.path())?;

        // Verify all values, especially the last one
        for (key, value) in keys2.iter().zip(values2.iter()) {
            let retrieved = db2.get(key).expect("Should retrieve value");
            assert_eq!(retrieved, value.as_slice(), "Value mismatch for key {:?}", key);
        }

        Ok(())
    }
}
