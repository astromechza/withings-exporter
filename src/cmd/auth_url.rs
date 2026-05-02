use anyhow::Result;
use rand::distr::{Alphanumeric, SampleString};

use crate::withings::client::authorize_url;

pub fn run(client_id: &str, redirect_uri: &str, scope: &str, state: Option<&str>) -> Result<()> {
    let st = state
        .map(str::to_string)
        .unwrap_or_else(|| Alphanumeric.sample_string(&mut rand::rng(), 24));
    println!("{}", authorize_url(client_id, redirect_uri, scope, &st));
    eprintln!("# state={st}");
    Ok(())
}
