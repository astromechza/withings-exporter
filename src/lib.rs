use anyhow::Result;

pub mod mappings;
pub mod state;

pub async fn run() -> Result<()> {
    println!("withings-exporter v{}", env!("CARGO_PKG_VERSION"));
    Ok(())
}
