pub mod activity;
pub mod intraday;
pub mod measure;
pub mod sleep;
pub mod workouts;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Envelope<B> {
    pub status: i64,
    pub body: Option<B>,
    #[serde(default)]
    pub error: Option<String>,
}

pub fn unwrap_envelope<B: for<'de> Deserialize<'de>>(json: &str) -> Result<B> {
    let env: Envelope<B> = serde_json::from_str(json).context("parse envelope")?;
    if env.status != 0 {
        bail!("withings status={} error={:?}", env.status, env.error);
    }
    env.body.context("body missing")
}
