use clap::Parser;
use ethryx::{run, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();

    let cfg = Config::parse();
    run(cfg, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    Ok(())
}
