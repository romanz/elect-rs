#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
// I really don't know why it fails without this line
extern crate configure_me;

pub mod app;
pub mod bulk;
pub mod cache;
pub mod config;
pub mod daemon;
pub mod errors;
pub mod fake;
pub mod index;
pub mod mempool;
pub mod metrics;
pub mod query;
pub mod rpc;
pub mod signal;
pub mod store;
pub mod sync;
pub mod util;
