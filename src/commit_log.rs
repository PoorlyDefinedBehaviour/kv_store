use crate::{
  constants::{
    CHECKSUM_SIZE_IN_BYTES, KEY_LEN_SIZE_IN_BYTES, TOMBSTONE_VALUE_LEN, VALUE_LEN_SIZE_IN_BYTES,
  },
  kv::LogStore,
};
use anyhow::Result;
use async_trait::async_trait;
use tokio::{fs::File, io::AsyncWriteExt, io::BufWriter};

pub struct CommitLog {
  /// Buffered file writer.
  file_writer: BufWriter<File>,
  /// Used to create a checksum from the key and value for every entry.
  checksum_algo: crc::Crc<u32>,
  /// The offset where the next entry will be written to in the log file.
  position: u64,
}

impl std::fmt::Debug for CommitLog {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("CommitLog")
      .field("file_writer", &self.file_writer)
      .field("position", &self.position)
      .finish()
  }
}

impl CommitLog {
  pub async fn new(dir: String) -> Result<Self> {
    Ok(Self {
      checksum_algo: crc::Crc::<u32>::new(&crc::CRC_32_CKSUM),
      file_writer: Self::create_data_file(&dir).await?,
      position: 0,
    })
  }

  async fn create_data_file(dir: &str) -> Result<BufWriter<File>> {
    let file = tokio::fs::OpenOptions::new()
      .read(true)
      .write(true)
      .append(true)
      .create(true)
      .open(format!(
        "{}/data.{}",
        dir,
        // If we already have a data file on disk, give the new one an id that's greater.
        // If not, start at id 0.
        Self::get_latest_data_file_id(dir)
          .await?
          .map(|id| id + 1)
          .unwrap_or(0)
      ))
      .await?;

    Ok(BufWriter::new(file))
  }

  async fn get_latest_data_file_id(dir: &str) -> Result<Option<u32>> {
    let files = std::fs::read_dir(dir)?
      .map(|entry| entry.map(|e| e.path()))
      .collect::<Result<Vec<_>, _>>()?
      .into_iter()
      .map(|path| path.to_str().map(String::from).unwrap());

    let latest_id = files
      .map(|path| path.split('/').last().map(String::from))
      .filter_map(|path| match path {
        None => None,
        Some(path) => {
          if path.starts_with("data.") {
            Some(path)
          } else {
            None
          }
        }
      })
      .map(|path| {
        path
          .split_once('.')
          .map(|(_, id)| id)
          .unwrap()
          .parse::<u32>()
          .unwrap()
      })
      .max();

    Ok(latest_id)
  }

  pub async fn flush(&mut self) -> Result<()> {
    self.file_writer.flush().await?;
    Ok(())
  }
}

#[async_trait]
impl LogStore for CommitLog {
  async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
    let checksum = {
      let mut buffer = Vec::with_capacity(key.len() + value.len());
      buffer.extend_from_slice(&key);
      buffer.extend_from_slice(&value);
      self.checksum_algo.checksum(&buffer)
    };

    self.file_writer.write_u32(checksum).await?;

    let key_len = key.len() as u32;
    self.file_writer.write_u32(key_len).await?;

    let value_len = value.len() as i32;
    self.file_writer.write_i32(value_len).await?;

    self.file_writer.write_all(&key).await?;
    self.file_writer.write_all(&value).await?;

    let entry_starts_at_position = self.position;

    self.position += CHECKSUM_SIZE_IN_BYTES
      + KEY_LEN_SIZE_IN_BYTES
      + VALUE_LEN_SIZE_IN_BYTES
      + key_len as u64
      + value_len as u64;

    Ok(entry_starts_at_position)
  }

  async fn delete(&mut self, key: &[u8]) -> Result<u64> {
    let checksum = self.checksum_algo.checksum(key);

    self.file_writer.write_u32(checksum).await?;

    let key_len = key.len() as u32;
    self.file_writer.write_u32(key_len).await?;

    self.file_writer.write_i32(TOMBSTONE_VALUE_LEN).await?;

    self.file_writer.write_all(key).await?;

    let entry_starts_at_position = self.position;

    self.position +=
      CHECKSUM_SIZE_IN_BYTES + KEY_LEN_SIZE_IN_BYTES + VALUE_LEN_SIZE_IN_BYTES + key_len as u64;

    Ok(entry_starts_at_position)
  }
}

#[cfg(test)]
mod new_tests {
  use crate::tests::support;

  use super::*;

  #[tokio::test]
  async fn creates_a_new_data_file_on_disk() -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = support::file::temp_dir();

    let _log = CommitLog::new(tempdir.path.clone()).await?;
    let _log = CommitLog::new(tempdir.path.clone()).await?;
    let _log = CommitLog::new(tempdir.path.clone()).await?;

    let files = support::file::list_dir_files(&tempdir.path);

    for expected in ["data.0", "data.1", "data.2"] {
      assert!(files.iter().any(|path| path.contains(&expected)));
    }

    Ok(())
  }
}

#[cfg(test)]
mod put_tests {
  use super::*;
  use crate::tests::support;
  use proptest::prelude::*;
  use tokio::{io::AsyncReadExt, runtime::Runtime};

  #[tokio::test]
  async fn returns_the_position_where_the_new_entry_starts(
  ) -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = support::file::temp_dir();

    let mut log = CommitLog::new(tempdir.path.clone()).await?;

    let key_1 = b"key_1".to_vec();
    let value_1 = b"value_1".to_vec();
    let offset = log.put(key_1.clone(), value_1.clone()).await?;
    // First entry starts at offset 0
    assert_eq!(0, offset);

    let key_2 = b"key13221312421_2".to_vec();
    let value_2 = b"value5212552121612_1".to_vec();
    let offset = log.put(key_2, value_2).await?;
    // Second entry starts after the first entry
    assert_eq!(
      CHECKSUM_SIZE_IN_BYTES
        + KEY_LEN_SIZE_IN_BYTES
        + VALUE_LEN_SIZE_IN_BYTES
        + key_1.len() as u64
        + value_1.len() as u64,
      offset
    );

    Ok(())
  }

  proptest! {
    #[test]
    fn writes_entries_to_disk(entries: Vec<(Vec<u8>, Vec<u8>)>) {
      Runtime::new().unwrap().block_on(async {
        let tempdir = support::file::temp_dir();

        let mut log = CommitLog::new(tempdir.path.clone()).await?;

        for (key, value) in entries.iter() {
          let _offset = log.put(key.clone(), value.clone()).await?;
        }

        log.flush().await?;

        let data_file_path = &support::file::list_dir_files(&tempdir.path)[0];

        let mut file = tokio::fs::File::open(data_file_path).await?;

        for (key, value) in entries.into_iter() {
          let _checksum = file.read_u32().await?;
          let key_len = file.read_u32().await?;
          let value_len = file.read_i32().await?;

          let mut key_buffer = vec![0; key_len as usize];
          file.read_exact(&mut key_buffer).await?;

          let mut value_buffer = vec![0; value_len as usize];
          file.read_exact(&mut value_buffer).await?;

          assert_eq!(key.len() as u32, key_len);
          assert_eq!(value.len() as i32, value_len);
          assert_eq!(key, key_buffer);
          assert_eq!(value, value_buffer);
        }

        Result::<()>::Ok(())
      })
      .unwrap()
    }
  }
}

#[cfg(test)]
mod delete_tests {

  use tokio::io::AsyncReadExt;

  use crate::tests::support;

  use super::*;

  #[tokio::test]
  async fn returns_the_offset_of_the_new_entry() -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = support::file::temp_dir();

    let mut log = CommitLog::new(tempdir.path.clone()).await?;

    let key = b"key".to_vec();
    let value = b"value".to_vec();
    let offset = log.put(key.clone(), value.clone()).await?;
    // First entry starts at offset 0
    assert_eq!(0, offset);

    let offset = log.delete(&key).await?;
    // Second entry starts after the first entry
    assert_eq!(
      CHECKSUM_SIZE_IN_BYTES
        + KEY_LEN_SIZE_IN_BYTES
        + VALUE_LEN_SIZE_IN_BYTES
        + key.len() as u64
        + value.len() as u64,
      offset
    );

    Ok(())
  }

  #[tokio::test]
  async fn deleted_entries_have_a_tombstone_value_length() -> Result<(), Box<dyn std::error::Error>>
  {
    let tempdir = support::file::temp_dir();

    let mut log = CommitLog::new(tempdir.path.clone()).await?;

    let key = b"key".to_vec();

    let _offset = log.delete(&key).await?;

    log.flush().await?;

    let mut file = tokio::fs::File::open(&support::file::list_dir_files(&tempdir.path)[0]).await?;

    let _checksum = file.read_u32().await?;
    let key_len = file.read_u32().await?;
    let value_len = file.read_i32().await?;

    let mut key_buffer = vec![0; key_len as usize];
    file.read_exact(&mut key_buffer).await?;

    assert_eq!(key.len() as u32, key_len);
    assert_eq!(TOMBSTONE_VALUE_LEN, value_len);
    assert_eq!(key, key_buffer);

    Ok(())
  }
}
