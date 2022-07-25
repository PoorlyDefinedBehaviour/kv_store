use crate::kv::Kv;
use async_trait::async_trait;
use tonic::{Request, Response, Status};
pub mod api {
  tonic::include_proto!("api");
}

use api::kv_store_server::KvStore;
use api::{DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse};

use crate::commit_log::CommitLog;

#[derive(Debug)]
pub struct Server {
  kv: Kv<CommitLog>,
}

impl Server {
  pub fn new(kv: Kv<CommitLog>) -> Self {
    Self { kv }
  }
}

#[async_trait]
impl KvStore for Server {
  async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
    let request = request.into_inner();

    match self.kv.put(request.key, request.value).await {
      Err(err) => Err(Status::unavailable(err.to_string())),
      Ok(()) => Ok(Response::new(PutResponse { ok: true })),
    }
  }

  async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
    let request = request.into_inner();

    match self.kv.get(&request.key).await {
      Err(err) => Err(Status::unavailable(err.to_string())),
      Ok(value) => Ok(Response::new(GetResponse {
        found: value.is_some(),
        value: value.unwrap_or_default(),
      })),
    }
  }

  async fn delete(
    &self,
    request: Request<DeleteRequest>,
  ) -> Result<Response<DeleteResponse>, Status> {
    let request = request.into_inner();

    match self.kv.delete(&request.key).await {
      Err(err) => Err(Status::unavailable(err.to_string())),
      Ok(deleted) => Ok(Response::new(DeleteResponse { deleted })),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use super::*;
  use crate::tests::support;
  use anyhow::Result;
  use proptest::prelude::*;
  use tokio::runtime::Runtime;

  #[derive(Debug, Clone, proptest_derive::Arbitrary)]
  enum Action {
    Put(Vec<u8>, Vec<u8>),
    Get(Vec<u8>),
    Delete(Vec<u8>),
  }

  impl Action {
    fn key(&self) -> &[u8] {
      match self {
        Action::Put(key, _) | Action::Get(key) | Action::Delete(key) => key,
      }
    }
  }

  proptest! {
    #[test]
    fn smoke(mut actions: Vec<Action>) {
      let put_actions = actions.clone().into_iter()
        .filter(|action| matches!(action, Action::Put(_, _)))
        .collect::<Vec<_>>();

      for action in put_actions.into_iter() {
        if rand::random() {
          actions.push(Action::Get(action.key().to_vec()));
        }

        if rand::random() {
          actions.push(Action::Delete(action.key().to_vec()));
        }
      }

      Runtime::new().unwrap().block_on(async {
        let tempdir = support::file::temp_dir();

        let kv = Kv::new(
          tempdir.path.clone(),
          CommitLog::new(tempdir.path.clone()).await?,
        )?;

        let mut client = support::grpc::start_server(kv).await;

        let mut keys_put = HashMap::new();

        for action in actions.into_iter() {
          match action {
            Action::Put(key, value) => {
              let _response = client
                .put(Request::new(PutRequest {
                  key: key.clone(),
                  value: value.clone(),
                }))
                .await?
                .into_inner();

              keys_put.insert(key, value);
            },
            Action::Get(key) => {
              let response = client
                .get(Request::new(GetRequest{ key: key.clone() }))
                .await?
                .into_inner();

              let value = keys_put.get(&key);

              if response.found || value.is_some() {
                assert_eq!(value, Some(&response.value));
              }
            },
            Action::Delete(key) => {
              let response = client
                .delete(Request::new(DeleteRequest{ key: key.clone() }))
                .await?
                .into_inner();

              assert_eq!(response.deleted, keys_put.get(&key).is_some());

              keys_put.remove(&key);
            }
          }
        }

        Result::<()>::Ok(())
      })
      .unwrap()
    }
  }
}
