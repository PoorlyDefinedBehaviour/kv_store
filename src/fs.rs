use anyhow::Result;

/// Lists files in a directory that have index. in their names.
pub async fn list_index_files(dir: &str) -> Result<Vec<String>> {
  let mut readdir = tokio::fs::read_dir(dir).await?;

  let mut paths = vec![];

  while let Some(entry) = readdir.next_entry().await? {
    if let Some(path) = entry.path().to_str().map(String::from) {
      if path.contains("index.") {
        paths.push(path);
      }
    }
  }

  Ok(paths)
}
