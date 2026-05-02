use anyhow::{Context, Result};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub client_id: String,
    pub client_secret: String,
    pub state_path: PathBuf,
    pub otlp_endpoint: String,
    pub backfill_days: i64,
    pub user_tz: String,
    pub user_agent: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_env_with_state(&PathBuf::from(
            std::env::var("WITHINGS_STATE_PATH").unwrap_or_else(|_| "/state/state.json".into()),
        ))
    }

    pub fn from_env_with_state(state_path: &std::path::Path) -> Result<Self> {
        Ok(Self {
            client_id: std::env::var("WITHINGS_CLIENT_ID").context("WITHINGS_CLIENT_ID")?,
            client_secret: std::env::var("WITHINGS_CLIENT_SECRET")
                .context("WITHINGS_CLIENT_SECRET")?,
            state_path: state_path.to_path_buf(),
            otlp_endpoint: std::env::var("OTLP_ENDPOINT")
                .unwrap_or_else(|_| "http://otel-collector.monitoring:4318".into()),
            backfill_days: std::env::var("WITHINGS_BACKFILL_DAYS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
            user_tz: std::env::var("WITHINGS_USER_TZ").unwrap_or_else(|_| "UTC".into()),
            user_agent: std::env::var("WITHINGS_USER_AGENT")
                .unwrap_or_else(|_| format!("withings-exporter/{}", env!("CARGO_PKG_VERSION"))),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_compile() {
        let c = Config {
            client_id: "x".into(),
            client_secret: "y".into(),
            state_path: PathBuf::from("/state/state.json"),
            otlp_endpoint: "http://otel-collector.monitoring:4318".into(),
            backfill_days: 30,
            user_tz: "UTC".into(),
            user_agent: "withings-exporter/0.1.0".into(),
        };
        assert_eq!(c.backfill_days, 30);
    }
}
