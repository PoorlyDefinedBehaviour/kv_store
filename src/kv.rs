use anyhow::Result;

use async_trait::async_trait;
use std::{
  collections::{BTreeMap, HashMap},
  io::SeekFrom,
};
use tokio::{
  fs::File,
  io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
  sync::RwLock,
};

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait LogStore {
  /// Appends a new entry to the log file and returns the offset where the entry begins.
  async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64>;
  /// Marks an entry as deleted.
  async fn delete(&mut self, key: &[u8]) -> Result<u64>;
}

#[derive(Debug, PartialEq)]
pub struct IndexEntry {
  pub key_len: u32,
  pub value_len: u32,
  pub file_id: u32,
  pub offset: u64,
}

#[derive(Debug)]
pub struct Kv<L: LogStore> {
  /// Directory where files will be stored.
  dir: String,
  inner: RwLock<KVInner<L>>,
}

#[derive(Debug)]
pub struct KVInner<L: LogStore> {
  memtable: BTreeMap<Vec<u8>, Vec<u8>>,
  /// 4000000000 / 20 byte key + 32 byte entry = 76 million keys in memory
  index: HashMap<Vec<u8>, IndexEntry>,
  commit_log: L,
  /// The id given to the latest sstable that was created.
  latest_ss_table_id: Option<u32>,
}

impl<L: LogStore> Kv<L> {
  pub fn new(dir: String, commit_log: L) -> Result<Self> {
    Ok(Self {
      inner: RwLock::new(KVInner {
        memtable: BTreeMap::new(),
        index: HashMap::new(),
        commit_log,
        latest_ss_table_id: Self::get_latest_ss_table_id(&dir)?,
      }),

      dir,
    })
  }

  fn get_latest_ss_table_id(dir: &str) -> Result<Option<u32>> {
    let files = std::fs::read_dir(dir)?
      .map(|entry| entry.map(|e| e.path()))
      .collect::<Result<Vec<_>, _>>()?
      .into_iter()
      .map(|path| path.to_str().map(String::from).unwrap());

    let latest_id = files
      .filter(|path| path.starts_with("sstable."))
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

  pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
    let mut inner = self.inner.write().await;

    let entry_starts_at_offset = inner.commit_log.put(key.clone(), value.clone()).await?;

    let file_id = inner.latest_ss_table_id.unwrap_or(0);

    inner.index.insert(
      key.clone(),
      IndexEntry {
        file_id,
        key_len: key.len() as u32,
        value_len: value.len() as u32,
        // We want the offset where the value starts at in the sstable file, so we
        // skip the number of bytes that come before the value.
        offset: entry_starts_at_offset + key.len() as u64,
      },
    );

    inner.memtable.insert(key, value);

    Ok(())
  }

  pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let inner = self.inner.read().await;

    match inner.memtable.get(key) {
      Some(value) => Ok(Some(value.clone())),
      None => {
        // Key is not in the memtable, maybe it is on disk.
        match inner.index.get(key) {
          None => Ok(None),
          Some(entry) => {
            // TODO: how slow is it to open a file? Maybe cache it.
            let mut file = File::open(format!("{}/sstable.{}", &self.dir, entry.file_id)).await?;
            file.seek(SeekFrom::Start(entry.offset)).await?;
            let mut buffer = vec![0; entry.value_len as usize];
            file.read_exact(&mut buffer).await?;
            Ok(Some(buffer))
          }
        }
      }
    }
  }

  pub async fn delete(&self, key: &[u8]) -> Result<bool> {
    let mut inner = self.inner.write().await;

    inner.commit_log.delete(key).await?;

    let key_exists = {
      let removed_from_memtable = inner.memtable.remove(key).is_some();
      let removed_from_index = inner.index.remove(key).is_some();
      removed_from_memtable || removed_from_index
    };

    Ok(key_exists)
  }

  /// Saves the data we have in memory on the disk.
  async fn flush_memtable_to_disk(&self) -> Result<()> {
    let mut inner = self.inner.write().await;

    let memtable = std::mem::take(&mut inner.memtable);

    // If we already have a sstable on disk, give the new one an id that's greater.
    // If not, start at id 0.
    let table_id = inner.latest_ss_table_id.map(|id| id + 1).unwrap_or(0);

    let mut ss_table_writer = BufWriter::new(self.create_ss_table_file(table_id).await?);
    let mut ss_index_writer = BufWriter::new(self.create_ss_index_file(table_id).await?);

    inner.latest_ss_table_id = Some(table_id);

    let mut offset = 0;

    // The memtable is a Btree and keys are sorted when we iterate over them
    // so the resulting file will have its contents sorted by key aka it is a sstable.
    for (key, value) in memtable.into_iter() {
      ss_table_writer.write_all(&key).await?;
      ss_table_writer.write_all(&value).await?;

      // Point offset to the value.
      offset += key.len() as u64;

      ss_index_writer.write_u32(key.len() as u32).await?;
      ss_index_writer.write_u32(value.len() as u32).await?;
      ss_index_writer.write_all(&key).await?;
      ss_index_writer.write_u64(offset).await?;

      // Skip the value.
      offset += value.len() as u64;
    }

    ss_table_writer.flush().await?;
    ss_index_writer.flush().await?;

    Ok(())
  }

  // Creates a file to save sstable contents.
  async fn create_ss_table_file(&self, table_id: u32) -> Result<File> {
    let file = tokio::fs::OpenOptions::new()
      .read(true)
      .write(true)
      .append(true)
      .create(true)
      .open(format!("{}/sstable.{}", self.dir, table_id))
      .await?;

    Ok(file)
  }

  async fn create_ss_index_file(&self, table_id: u32) -> Result<File> {
    let file = tokio::fs::OpenOptions::new()
      .read(true)
      .write(true)
      .append(true)
      .create(true)
      .open(format!("{}/index.{}", self.dir, table_id))
      .await?;

    Ok(file)
  }

  async fn merge_ss_index_files(&self) {
    // TODO: parallel merge files on disk
  }
}

#[cfg(test)]
mod put_tests {
  use crate::{
    commit_log::CommitLog,
    constants::{CHECKSUM_SIZE_IN_BYTES, KEY_LEN_SIZE_IN_BYTES, VALUE_LEN_SIZE_IN_BYTES},
    tests::support,
  };

  use super::*;
  use mockall::predicate::eq;

  #[tokio::test]
  async fn stores_entry_in_commit_log() -> Result<()> {
    let tempdir = support::file::temp_dir();

    let key = b"key".to_vec();
    let value = b"value".to_vec();

    let mut log = MockLogStore::new();

    log
      .expect_put()
      .times(1)
      .with(eq(key.clone()), eq(value.clone()))
      .return_once(|_, _| Ok(0));

    let kv = Kv::new(tempdir.path.clone(), log)?;

    kv.put(key, value).await?;

    Ok(())
  }

  #[tokio::test]
  async fn new_entries_are_added_to_the_index() -> Result<()> {
    let tempdir = support::file::temp_dir();

    let key = b"key".to_vec();
    let value = b"value".to_vec();

    let kv = Kv::new(
      tempdir.path.clone(),
      CommitLog::new(tempdir.path.clone()).await?,
    )?;

    assert!(kv.inner.read().await.index.is_empty());

    kv.put(key.clone(), value.clone()).await?;

    assert_eq!(
      kv.inner.read().await.index.get(&key),
      Some(&IndexEntry {
        file_id: 0,
        key_len: key.len() as u32,
        value_len: value.len() as u32,
        offset: CHECKSUM_SIZE_IN_BYTES
          + KEY_LEN_SIZE_IN_BYTES
          + VALUE_LEN_SIZE_IN_BYTES
          + key.len() as u64
      })
    );

    Ok(())
  }
}

#[cfg(test)]
mod get_latest_ss_table_id_tests {
  use crate::tests::support;

  use super::*;

  #[tokio::test]
  async fn returns_the_id_of_the_newest_ss_table_on_disk() -> Result<(), Box<dyn std::error::Error>>
  {
    let tempdir = support::file::temp_dir();

    {
      let kv = Kv::new(tempdir.path.clone(), MockLogStore::new())?;

      // Started with no ss tables.
      assert_eq!(None, kv.inner.read().await.latest_ss_table_id);

      kv.put(b"key".to_vec(), b"value".to_vec()).await?;
      kv.flush_memtable_to_disk().await?;
    }

    {
      let kv = Kv::new(tempdir.path.clone(), MockLogStore::new())?;

      // Flushed to disk once, so there's a sstable.
      assert_eq!(Some(0), kv.inner.read().await.latest_ss_table_id);

      kv.put(b"key".to_vec(), b"value".to_vec()).await?;
      kv.flush_memtable_to_disk().await?;
    }

    {
      let kv = Kv::new(tempdir.path.clone(), MockLogStore::new())?;
      // Flushed to disk twice, so there's two sstables.
      assert_eq!(Some(1), kv.inner.read().await.latest_ss_table_id);
    }

    Ok(())
  }
}

#[cfg(test)]
mod get_tests {
  use super::*;
  use crate::{commit_log::CommitLog, tests::support};
  use proptest::prelude::*;
  use tokio::runtime::Runtime;

  #[tokio::test]
  async fn reads_value_from_ss_table_if_it_is_not_in_the_memtable(
  ) -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = support::file::temp_dir();

    let kv = Kv::new(
      tempdir.path.clone(),
      CommitLog::new(tempdir.path.clone()).await?,
    )?;

    let key = b"key".to_vec();
    let value = b"value".to_vec();

    // Key will be in the memtable
    kv.put(key.clone(), value.clone()).await?;

    // Create sstable and clean memtable.
    kv.flush_memtable_to_disk().await?;

    assert!(kv.inner.read().await.memtable.is_empty());

    // Should read value from sstable key is not in the memtable.
    assert_eq!(Some(value), kv.get(&key).await?);

    Ok(())
  }

  proptest! {
    #[test]
    fn returns_value_associated_to_key(key: String, value: String) {
      Runtime::new().unwrap().block_on(async {
        let tempdir = support::file::temp_dir();

        let key = key.as_bytes().to_vec();
        let value = value.as_bytes().to_vec();
        let mut log = MockLogStore::new();

        log.expect_put().times(1).return_once(|_, _| Ok(0));

        let kv = Kv::new(tempdir.path.clone(),log)?;

        kv.put(key.clone(), value.clone()).await?;

        assert_eq!(Some(value), kv.get(&key).await?);

        Result::<()>::Ok(())
      })
      .unwrap()
    }

    #[test]
    fn returns_none_when_key_is_not_found(data: Vec<(String, String)>, key: String) {
      // Ensure key won't be in the memtable.
      prop_assume!(!data.iter().any(|(k, _value)| k == &key));

      Runtime::new().unwrap().block_on(async {
        let tempdir = support::file::temp_dir();

        let kv = Kv::new(tempdir.path.clone(), CommitLog::new(tempdir.path.clone()).await?)?;

        for (key, value) in data.into_iter() {
          kv.put(key.as_bytes().to_vec(), value.as_bytes().to_vec()).await?;
        }

        assert_eq!(None, kv.get(key.as_bytes()).await?);

        Result::<()>::Ok(())
      })
      .unwrap()
    }
  }
}

#[cfg(test)]
mod flush_memtable_to_disk_tests {
  use std::io::SeekFrom;

  use proptest::prelude::*;
  use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    runtime::Runtime,
  };

  use crate::{commit_log::CommitLog, tests::support};

  use super::*;

  #[tokio::test]
  async fn creates_ss_table_and_index_files_with_monotonically_increasing_ids(
  ) -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = support::file::temp_dir();

    let kv = Kv::new(
      tempdir.path.clone(),
      CommitLog::new(tempdir.path.clone()).await?,
    )?;

    kv.flush_memtable_to_disk().await?;
    kv.flush_memtable_to_disk().await?;
    kv.flush_memtable_to_disk().await?;

    let files = support::file::list_dir_files(&tempdir.path);

    // Each sstable has its own index.
    for expected in [
      "sstable.0",
      "index.0",
      "sstable.1",
      "index.1",
      "sstable.2",
      "index.2",
    ] {
      assert!(files.iter().any(|path| path.contains(expected)));
    }

    Ok(())
  }

  #[tokio::test]
  async fn writes_ss_table_to_disk() -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = support::file::temp_dir();

    let kv = Kv::new(
      tempdir.path.clone(),
      CommitLog::new(tempdir.path.clone()).await?,
    )?;

    kv.put(b"key".to_vec(), b"value".to_vec()).await?;

    kv.flush_memtable_to_disk().await?;

    let files = support::file::list_dir_files(&tempdir.path);

    assert!(files.into_iter().any(|path| path.contains("sstable.0")));

    Ok(())
  }

  #[tokio::test]
  async fn mem_table_is_empty_after_ss_table_is_written_to_disk(
  ) -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = support::file::temp_dir();

    let kv = Kv::new(
      tempdir.path.clone(),
      CommitLog::new(tempdir.path.clone()).await?,
    )?;

    kv.put(b"key".to_vec(), b"value".to_vec()).await?;

    assert!(!kv.inner.read().await.memtable.is_empty());

    kv.flush_memtable_to_disk().await?;

    assert!(kv.inner.read().await.memtable.is_empty());

    Ok(())
  }

  proptest! {
    #[test]
    fn index_file_contains_offsets_pointing_to_the_ss_table(
      mut entries: Vec<(Vec<u8>, Vec<u8>)>
    )  {
      let mut entries = entries.into_iter().filter(|(key, _value)| !key.is_empty()).collect::<Vec<_>>();
      // Index will be sorted on disk.
      entries.sort_by_key(|(key, _value)| key.clone());

      Runtime::new().unwrap().block_on(async {
        let tempdir = support::file::temp_dir();

        let kv = Kv::new(
          tempdir.path.clone(),
          CommitLog::new(tempdir.path.clone()).await?,
        )?;

        for (key, value) in entries.iter() {
          kv.put(key.clone(), value.clone()).await?;
        }

        kv.flush_memtable_to_disk().await?;

        let files = support::file::list_dir_files(&tempdir.path);

        let mut index = File::open(files.iter().find(|path| path.contains("index")).unwrap()).await?;
        let mut sstable = File::open(files.iter().find(|path| path.contains("sstable")).unwrap()).await?;

        for (key, value) in entries.into_iter(){
          let key_len = index.read_u32().await?;
          assert_eq!(key.len() as u32, key_len);

          let value_len = index.read_u32().await?;
          assert_eq!(value.len() as u32, value_len);

          let mut key_buffer = vec![0; key_len as usize];
          index.read_exact(&mut key_buffer).await?;
          assert_eq!(key, key_buffer);

          // Grab the offset from the index and read the value from the sstable.
          let offset = index.read_u64().await?;

          sstable.seek(SeekFrom::Start(offset)).await?;

          let mut value_buffer = vec![0; value_len as usize];
          sstable.read_exact(&mut value_buffer).await?;

          assert_eq!(value, value_buffer);
        }

        Result::<()>::Ok(())
      })
      .unwrap();
    }
  }

  #[tokio::test]
  async fn ss_tables_are_merged_after_some_amount_of_time() -> Result<(), Box<dyn std::error::Error>>
  {
    let tempdir = support::file::temp_dir();

    let kv = Kv::new(
      tempdir.path.clone(),
      CommitLog::new(tempdir.path.clone()).await?,
    )?;

    kv.put(b"key_1".to_vec(), b"value_1".to_vec()).await?;

    // Create the first sstable.
    kv.flush_memtable_to_disk().await?;

    kv.put(b"key_2".to_vec(), b"value_2".to_vec()).await?;

    // Create the second sstable.
    kv.flush_memtable_to_disk().await?;

    let files = support::file::list_dir_files(&tempdir.path);

    dbg!(&files);

    Ok(())
  }
}

#[cfg(test)]
mod delete_tests {

  use crate::{commit_log::CommitLog, tests::support};

  use super::*;

  #[tokio::test]
  async fn appends_deletion_to_commit_log() -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = support::file::temp_dir();

    let mut log = MockLogStore::new();

    let key = b"key".to_vec();

    {
      let key = key.clone();
      log.expect_delete().times(1).returning(move |input| {
        assert_eq!(key, input);
        Ok(0)
      });
    }

    let kv = Kv::new(tempdir.path.clone(), log)?;

    let _ = kv.delete(&key).await?;

    Ok(())
  }

  #[tokio::test]
  async fn returns_false_when_key_does_not_exist() -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = support::file::temp_dir();

    let kv = Kv::new(
      tempdir.path.clone(),
      CommitLog::new(tempdir.path.clone()).await?,
    )?;

    assert!(!kv.delete(b"key").await?);

    Ok(())
  }

  #[tokio::test]
  async fn returns_true_if_key_was_in_the_memtable_when_deletion_happened(
  ) -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = support::file::temp_dir();

    let kv = Kv::new(
      tempdir.path.clone(),
      CommitLog::new(tempdir.path.clone()).await?,
    )?;

    let key = b"key".to_vec();
    let value = b"value".to_vec();

    // Key will be in the memtable because it was just added.
    kv.put(key.clone(), value).await?;

    // Should return true because the key is in the memtable.
    assert!(kv.delete(&key).await?);

    Ok(())
  }

  #[tokio::test]
  async fn returns_true_if_key_was_in_the_index_but_no_in_the_memtable_when_deletion_happened(
  ) -> Result<(), Box<dyn std::error::Error>> {
    let tempdir = support::file::temp_dir();

    let kv = Kv::new(
      tempdir.path.clone(),
      CommitLog::new(tempdir.path.clone()).await?,
    )?;

    let key = b"key".to_vec();
    let value = b"value".to_vec();

    // Key will be in the memtable because it was just added.
    kv.put(key.clone(), value).await?;

    // Memtable is cleared when it is flushed to disk.
    kv.flush_memtable_to_disk().await?;

    assert!(kv.inner.read().await.memtable.is_empty());

    // Should return true because the key is in the index.
    assert!(kv.delete(&key).await?);

    Ok(())
  }
}
