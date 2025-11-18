use std::time::Duration;

use anyhow::Result;
use carabiner::{AppConfig, instances};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let config = AppConfig::load()?;
    let database_url = config.database_url;
    tracing::info!("starting worker controller loop");
    instances::wait_for_instance_poll(&database_url, Duration::from_secs(1)).await?;
    Ok(())
}
