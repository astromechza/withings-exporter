use anyhow::Result;

pub mod config;
pub mod mappings;
pub mod metrics;
pub mod otlp;
pub mod state;
pub mod withings;

pub async fn run() -> Result<()> {
    println!("withings-exporter v{}", env!("CARGO_PKG_VERSION"));
    Ok(())
}
