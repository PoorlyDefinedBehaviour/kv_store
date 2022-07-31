use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, EnvFilter, Registry};
use tracing_tree::HierarchicalLayer;

mod commit_log;
mod constants;
mod fs;
mod kv;
mod server;

#[cfg(test)]
mod tests;

fn main() {
  init_logging();
}

fn init_logging() {
  let (non_blocking_writer, _guard) = tracing_appender::non_blocking(std::io::stdout());

  let app_name = concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION")).to_string();

  if cfg!(test) {
    std::env::set_var("RUST_LOG", format!("{}=trace", env!("CARGO_PKG_NAME")));
  }

  let bunyan_formatting_layer = BunyanFormattingLayer::new(app_name, non_blocking_writer);

  let subscriber = Registry::default()
    .with(EnvFilter::from_env("RUST_LOG"))
    .with(JsonStorageLayer)
    .with(HierarchicalLayer::new(1))
    .with(bunyan_formatting_layer);

  tracing::subscriber::set_global_default(subscriber).unwrap();
}
