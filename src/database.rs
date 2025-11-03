use crate::header::DabaHeader;
use boomphf::Mphf;
use memmap2::Mmap;
use std::path::Path;
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    sync::Arc,
};

pub const KEY_SIZE: usize = 16;
pub type Key = [u8; KEY_SIZE];

pub struct Database {
    header: DabaHeader,
    mmap_data: Arc<Mmap>,   // mmap of values
    offsets: Arc<Vec<u64>>, // offsets array in memory
    mphf: Mphf<Key>,        // minimal perfect hash of keys
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
        let file = File::open(data_file)?;
        let mmap_data = unsafe { Mmap::map(&file)? };

        let header_size = std::mem::size_of::<DabaHeader>();
        let header = DabaHeader::from_bytes(&mmap_data[0..header_size])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid DABA header"))?;

        let num_keys = header.num_keys as usize;

        let mut buf = Vec::new();
        let mut idx_file = File::open(index_file)?;
        idx_file.read_to_end(&mut buf)?;

        let mut keys = Vec::with_capacity(num_keys);
        let mut offsets = Vec::with_capacity(num_keys);
        let mut cursor = 0;

        for _ in 0..num_keys {
            let key_bytes: Key = buf[cursor..cursor + KEY_SIZE].try_into().unwrap();
            cursor += KEY_SIZE;
            keys.push(key_bytes);

            let off = u64::from_le_bytes(buf[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            offsets.push(off);
        }

        let mphf = Mphf::new(1.7, &keys);

        Ok(Self {
            mmap_data: Arc::new(mmap_data),
            offsets: Arc::new(offsets),
            mphf,
            header,
        })
    }

    pub fn get(&self, key: &Key) -> Option<&[u8]> {
        let idx = self.mphf.try_hash(key)? as usize;

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

        let mphf = Mphf::new(1.7, &keys_vec);

        let mut mphf_to_original = vec![0usize; keys_vec.len()];
        for (original_idx, key) in keys_vec.iter().enumerate() {
            let mphf_idx = mphf.hash(key) as usize;
            mphf_to_original[mphf_idx] = original_idx;
        }

        // Write data in the order determined by MPHF
        for mphf_idx in 0..keys_vec.len() {
            let original_idx = mphf_to_original[mphf_idx];
            let val_bytes = &values_vec[original_idx];
            data_file.write_all(val_bytes)?;
        }

        let mut index_file = File::create(&path_index)?;
        let mut cursor = 0u64;
        for mphf_idx in 0..keys_vec.len() {
            let original_idx = mphf_to_original[mphf_idx];
            let key = &keys_vec[original_idx];

            index_file.write_all(key)?;
            index_file.write_all(&cursor.to_le_bytes())?;

            cursor += values_vec[original_idx].len() as u64;
        }

        let header = DabaHeader {
            magic: *b"DABA",
            version,
            num_keys,
            key_size: KEY_SIZE as u64,
            values_start: header_size as usize,
        };
        data_file.seek(SeekFrom::Start(0))?;
        data_file.write_all(&bincode::serialize(&header).unwrap())?;

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
}
