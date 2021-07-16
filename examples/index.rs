/// Benchmark regular indexing flow (using JSONRPC), don't persist the resulting index.
extern crate electrs;
extern crate error_chain;

#[macro_use]
extern crate log;

use electrs::sync::Arc;
use electrs::{
    cache::BlockTxIDsCache, config::Config, daemon::Daemon, errors::*, fake::FakeStore,
    index::Index, metrics::Metrics, signal::Waiter,
};
use error_chain::ChainedError;

fn run() -> Result<()> {
    let signal = Waiter::start();
    let config = Config::from_args();
    let metrics = Metrics::new(config.monitoring_addr);
    metrics.start();
    let cache = Arc::new(BlockTxIDsCache::new(0, &metrics));

    let daemon = Daemon::new(
        &config.daemon_dir,
        &config.blocks_dir,
        config.daemon_rpc_addr,
        config.cookie_getter(),
        config.network_type,
        signal.clone(),
        cache,
        &metrics,
    )?;
    let fake_store = FakeStore {};
    let index = Index::load(&fake_store, &daemon, &metrics, config.index_batch_size)?;
    index.update(&fake_store, &signal)?;
    Ok(())
}

fn main() {
    if let Err(e) = run() {
        error!("{}", e.display_chain());
    }
}
