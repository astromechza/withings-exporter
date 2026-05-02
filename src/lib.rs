use anyhow::Result;
use clap::Parser;

pub mod cli;
pub mod cmd;
pub mod config;
pub mod mappings;
pub mod metrics;
pub mod otlp;
pub mod poll;
pub mod state;
pub mod withings;

pub async fn run() -> Result<()> {
    init_logging();
    let cli = cli::Cli::parse();
    match cli.command {
        cli::Cmd::AuthUrl {
            client_id,
            redirect_uri,
            scope,
            state,
        } => cmd::auth_url::run(&client_id, &redirect_uri, &scope, state.as_deref()),
        cli::Cmd::Exchange {
            client_id,
            client_secret,
            redirect_uri,
            code,
            state_file,
        } => {
            cmd::exchange::run(
                &client_id,
                &client_secret,
                &redirect_uri,
                &code,
                &state_file,
            )
            .await
        }
        cli::Cmd::Poll => cmd::poll_cmd::run().await,
        cli::Cmd::DumpState { state_file } => cmd::dump_state::run(&state_file),
    }
}

fn init_logging() {
    use tracing_subscriber::EnvFilter;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();
}
