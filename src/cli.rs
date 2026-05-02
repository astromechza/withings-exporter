use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "withings-exporter", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Print an OAuth authorize URL to open in a browser.
    AuthUrl {
        #[arg(long, env = "WITHINGS_CLIENT_ID")]
        client_id: String,
        #[arg(long)]
        redirect_uri: String,
        #[arg(long, default_value = "user.metrics,user.activity,user.info")]
        scope: String,
        #[arg(long)]
        state: Option<String>,
    },
    /// Exchange an auth code for tokens and write initial state file.
    Exchange {
        #[arg(long, env = "WITHINGS_CLIENT_ID")]
        client_id: String,
        #[arg(long, env = "WITHINGS_CLIENT_SECRET")]
        client_secret: String,
        #[arg(long)]
        redirect_uri: String,
        #[arg(long)]
        code: String,
        #[arg(long, default_value = "./state.json")]
        state_file: PathBuf,
    },
    /// Run a single poll cycle: refresh → fetch → push → save state.
    Poll {
        #[arg(long, env = "WITHINGS_STATE_PATH", default_value = "/state/state.json")]
        state_file: PathBuf,
    },
    /// Print state.json with secrets redacted.
    DumpState {
        #[arg(long, env = "WITHINGS_STATE_PATH", default_value = "/state/state.json")]
        state_file: PathBuf,
    },
}
