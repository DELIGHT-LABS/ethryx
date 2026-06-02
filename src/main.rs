use clap::Parser;
use ethryx::{Config, run};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = Config::parse();

    // RUST_LOG (if set) wins and allows per-target directives; otherwise build the
    // filter from --log-level plus the access-log toggle. The access log lives on
    // its own `access_log` target — deliberately *not* a child of `ethryx`, so
    // `RUST_LOG=ethryx=info` doesn't pull it in. It's off unless `--access-log`
    // raises it to info; RUST_LOG can target it directly (`access_log=info`).
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        let access = if cfg.access_log { "info" } else { "off" };
        tracing_subscriber::EnvFilter::new(format!("{},access_log={access}", cfg.log_level))
    });
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
