use uuid::Uuid;

#[derive(Debug)]
pub struct TempDir {
  pub path: String,
}

impl Drop for TempDir {
  fn drop(&mut self) {
    std::fs::remove_dir_all(&self.path).unwrap();
  }
}

#[cfg(test)]
pub fn temp_dir() -> TempDir {
  let path = format!(
    "{}/{}/{}",
    std::env::temp_dir().as_path().to_str().unwrap(),
    "testing_dir",
    Uuid::new_v4()
  );

  std::fs::create_dir_all(&path).unwrap();

  TempDir { path }
}

#[cfg(test)]
pub fn list_dir_files(dir: &str) -> Vec<String> {
  std::fs::read_dir(dir)
    .expect("error reading files in directory")
    .map(|entry| entry.map(|e| e.path()))
    .collect::<Result<Vec<_>, _>>()
    .expect("error reading directory entries")
    .into_iter()
    .map(|entry| entry.to_str().map(String::from).unwrap())
    .collect()
}
