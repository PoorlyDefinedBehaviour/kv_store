use crate::{commit_log::CommitLog, kv::Kv};
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::{Channel, Server};

use crate::server::api;

/// Starts the grpc server in a random port and returns a client that will make requests to it.
///
/// This function is useful for tests that need the server to be running.
#[cfg(test)]
pub async fn start_server(kv: Kv<CommitLog>) -> api::kv_store_client::KvStoreClient<Channel> {
  let svc = api::kv_store_server::KvStoreServer::new(crate::server::Server::new(kv));

  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap();

  tokio::spawn(async move {
    Server::builder()
      .timeout(Duration::from_millis(5000))
      .add_service(svc)
      .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
      .await
      .unwrap();
  });

  api::kv_store_client::KvStoreClient::connect(format!("http://{}", addr))
    .await
    .unwrap()
}
