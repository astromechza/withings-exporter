use anyhow::Result;
use jiff::Timestamp;
use reqwest::Client as HttpClient;
use std::path::Path;

use crate::state::{State, Tokens};
use crate::withings::client::WithingsClient;

pub async fn run(
    client_id: &str,
    client_secret: &str,
    redirect_uri: &str,
    code: &str,
    state_file: &Path,
) -> Result<()> {
    let http = HttpClient::builder()
        .user_agent(format!("withings-exporter/{}", env!("CARGO_PKG_VERSION")))
        .build()?;
    let stub = Tokens {
        access_token: String::new(),
        refresh_token: String::new(),
        expires_at: 0,
        scope: String::new(),
        userid: String::new(),
    };
    let client = WithingsClient::new(http, client_id.into(), client_secret.into(), stub);
    let body = client.exchange_code(code, redirect_uri).await?;
    let now = Timestamp::now().as_second();
    let state = State {
        tokens: Tokens {
            access_token: body.access_token,
            refresh_token: body.refresh_token,
            expires_at: now + body.expires_in,
            scope: body.scope,
            userid: match body.userid {
                serde_json::Value::String(s) => s,
                serde_json::Value::Number(n) => n.to_string(),
                v => v.to_string(),
            },
        },
        cursors: Default::default(),
        lifetime_counters: Default::default(),
        finalized_days_emitted: Default::default(),
        emitted_record_ids: Default::default(),
    };
    crate::state::save(state_file, &state)?;
    eprintln!("Wrote {}", state_file.display());
    Ok(())
}
