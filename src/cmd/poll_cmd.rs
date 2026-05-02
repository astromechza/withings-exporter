use anyhow::{Context, Result};
use opentelemetry::metrics::MeterProvider;
use reqwest::Client as HttpClient;

use crate::config::Config;
use crate::metrics::Instruments;
use crate::otlp;
use crate::poll::run_poll;
use crate::state;
use crate::withings::client::WithingsClient;

pub async fn run() -> Result<()> {
    let cfg = Config::from_env()?;
    let mut state =
        state::load(&cfg.state_path).context("load state — bootstrap with `exchange`?")?;
    let provider = otlp::init(&cfg.otlp_endpoint, &state.tokens.userid)?;
    let meter = provider.meter("withings-exporter");
    let inst = Instruments::new(meter);

    let http = HttpClient::builder()
        .user_agent(cfg.user_agent.clone())
        .build()?;
    let client = WithingsClient::new(
        http,
        cfg.client_id.clone(),
        cfg.client_secret.clone(),
        state.tokens.clone(),
    );

    let result = run_poll(&cfg, &client, &inst, &mut state).await;

    state.tokens = client.snapshot_tokens();
    state::save(&cfg.state_path, &state).context("save state")?;
    otlp::shutdown(provider).await?;
    result
}
