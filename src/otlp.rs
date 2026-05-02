use anyhow::{Context, Result};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::Resource;

pub fn init(otlp_endpoint: &str, userid: &str) -> Result<SdkMeterProvider> {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(format!(
            "{}/v1/metrics",
            otlp_endpoint.trim_end_matches('/')
        ))
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
        .build()
        .context("build OTLP metrics exporter")?;

    let resource = Resource::new([
        KeyValue::new("service.name", "withings-exporter"),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        KeyValue::new("withings.user_id", userid.to_string()),
    ]);

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(
        exporter,
        opentelemetry_sdk::runtime::Tokio,
    )
    .build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build();

    opentelemetry::global::set_meter_provider(provider.clone());
    Ok(provider)
}

pub async fn shutdown(provider: SdkMeterProvider) -> Result<()> {
    // force_flush and shutdown internally call futures_executor::block_on, which
    // panics if called from within an existing async runtime context. Use
    // spawn_blocking so they run on a dedicated thread pool thread instead.
    tokio::task::spawn_blocking(move || {
        provider.force_flush().context("force_flush")?;
        provider.shutdown().context("shutdown")?;
        Ok(())
    })
    .await
    .context("shutdown task")?
}
