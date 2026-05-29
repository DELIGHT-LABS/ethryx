use clap::Parser;
use ethryx::{Config, run};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = Config::parse();

    // RUST_LOG (if set) wins and allows per-target directives; otherwise fall
    // back to the --log-level flag.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cfg.log_level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();

    run(cfg, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    Ok(())
}
