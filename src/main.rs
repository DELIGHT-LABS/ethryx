use clap::Parser;
use ethryx::{Config, run};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = Config::parse();

    use tracing_subscriber::prelude::*;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        let access = if cfg.access_log { "info" } else { "off" };
        tracing_subscriber::EnvFilter::new(format!("{},access_log={access}", cfg.log_level))
    });

    #[cfg(feature = "otel")]
    {
        if let Some(ref _otel_endpoint) = cfg.otel_endpoint {
            let tracer = opentelemetry::global::tracer("ethryx");
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            tracing_subscriber::registry()
                .with(filter)
                .with(otel_layer)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        } else {
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        }
    }
    #[cfg(not(feature = "otel"))]
    {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    }

    run(cfg, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    Ok(())
}
