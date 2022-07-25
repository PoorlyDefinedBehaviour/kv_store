use anyhow::Result;

pub async fn list_index_files(&dir) -> Result<Vec<String>> {
  let readdir = tokio::fs::read_dir(dir).await?;

  let mut paths= vec![];

  while let Some(entry) = readdir.next_entry().await? {
    if let Some(path) =  entry.path().to_str().map(String::from) {
      if path.contains("index.") {
      paths.push(path);
      }
    }
  }
  

  Ok(paths)
}