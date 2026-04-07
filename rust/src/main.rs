//! Http server

use std::io;

use anyhow::Result;
use tracing_subscriber::{filter::EnvFilter, filter::LevelFilter, fmt, prelude::*};

use codecrafters_redis::server;

const SERVER_ADDRESS: &str = "127.0.0.1:6379";

#[tokio::main]
async fn main() -> Result<()> {

    // Set up logging
    let format = fmt::format()
        .without_time()
        .with_level(true) // include debug level in formatted output
        .with_target(true) // include targets (module)
        .with_ansi(false) // no pretty colours
        .compact(); // use the `Compact` formatting style.
    let format_layer = tracing_subscriber::fmt::layer()
        .with_writer(io::stderr)
        .event_format(format);
    let filter = EnvFilter::from_default_env().add_directive(LevelFilter::INFO.into());
    tracing_subscriber::registry()
        .with(format_layer)
        .with(filter)
        .init();

    server::run(SERVER_ADDRESS).await
    
}


