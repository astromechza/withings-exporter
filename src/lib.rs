use anyhow::Result;

pub async fn run() -> Result<()> {
    println!("withings-exporter v{}", env!("CARGO_PKG_VERSION"));
    Ok(())
}
