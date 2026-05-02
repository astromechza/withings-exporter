use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    withings_exporter::run().await
}
