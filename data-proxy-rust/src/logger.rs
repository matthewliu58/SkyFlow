use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};
use std::fs::OpenOptions;
use std::path::Path;

/// Initialize logger (equivalent to Go's custom slog Handler)
pub fn init_logger() -> Result<(), std::io::Error> {
    // Create log directory
    let log_dir = "log";
    std::fs::create_dir_all(log_dir)?;

    // Open log file
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .write(true)
        .open(Path::new(log_dir).join("app.log"))?;

    // Configure log subscriber (includes filename, line number, function name)
    tracing_subscriber::registry()
        .with(
            EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
                .add_directive("hyper=warn".parse().unwrap())
                .add_directive("tokio=warn".parse().unwrap()),
        )
        .with(
            fmt::layer()
                .with_ansi(false)
                .with_writer(log_file)
                .with_file(true)
                .with_line_number(true)
                // Remove this line: with_function_name(true) → not supported in newer version
                .with_span_events(FmtSpan::CLOSE),
        )
        .init();

    Ok(())
}