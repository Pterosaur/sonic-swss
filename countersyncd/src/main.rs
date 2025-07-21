// Application modules
mod message;
mod actor;

// External dependencies
use clap::Parser;
use log::{error, info};
use std::time::Duration;
use tokio::{spawn, sync::mpsc::channel};

// Internal actor implementations
use actor::{
    netlink::{NetlinkActor, get_genl_family_group}, 
    ipfix::IpfixActor,
    stats_reporter::{StatsReporterActor, StatsReporterConfig, ConsoleWriter},
    swss::SwssActor,
};

/// Initialize logging based on command line arguments
fn init_logging(log_level: &str, log_format: &str) {
    use env_logger::{Builder, Target, WriteStyle};
    use log::LevelFilter;
    use std::io::Write;

    let level = match log_level.to_lowercase().as_str() {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => {
            eprintln!("Invalid log level '{}', using 'info'", log_level);
            LevelFilter::Info
        }
    };

    let mut builder = Builder::new();
    builder.filter_level(level);
    builder.target(Target::Stdout);
    builder.write_style(WriteStyle::Auto);

    match log_format.to_lowercase().as_str() {
        "simple" => {
            builder.format(|buf, record| {
                writeln!(buf, "[{}] {}", record.level(), record.args())
            });
        }
        "full" => {
            builder.format(|buf, record| {
                writeln!(
                    buf,
                    "[{}] [{}:{}] [{}] {}",
                    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    record.level(),
                    record.args()
                )
            });
        }
        _ => {
            eprintln!("Invalid log format '{}', using 'full'", log_format);
            builder.format(|buf, record| {
                writeln!(
                    buf,
                    "[{}] [{}:{}] [{}] {}",
                    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    record.level(),
                    record.args()
                )
            });
        }
    }

    builder.init();
}

/// SONiC High Frequency Telemetry Counter Sync Daemon
/// 
/// This application processes high-frequency telemetry data from SONiC switches,
/// converting netlink messages and SWSS state database updates through IPFIX format to SAI statistics.
/// 
/// The application consists of four main actors:
/// - NetlinkActor: Receives raw netlink messages from the kernel
/// - SwssActor: Monitors SONiC orchestrator messages via state database for IPFIX templates
/// - IpfixActor: Processes IPFIX templates and data records to extract SAI stats  
/// - StatsReporterActor: Reports processed statistics to the console
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Enable stats reporting to console
    #[arg(short, long, default_value = "false")]
    enable_stats: bool,

    /// Stats reporting interval in seconds
    #[arg(short = 'i', long, default_value = "10")]
    stats_interval: u64,

    /// Show detailed statistics in reports
    #[arg(short = 'd', long, default_value = "true")]
    detailed_stats: bool,

    /// Maximum number of stats per report (0 for unlimited)
    #[arg(short = 'm', long, default_value = "20")]
    max_stats_per_report: u32,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short = 'l', long, default_value = "info", help = "Set the logging level")]
    log_level: String,

    /// Log format (simple, full)
    #[arg(long, default_value = "full", help = "Set the log output format: 'simple' for level and message only, 'full' for timestamp, file, line, level, and message")]
    log_format: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let args = Args::parse();
    
    // Initialize logging based on command line arguments
    init_logging(&args.log_level, &args.log_format);
    
    info!("Starting SONiC High Frequency Telemetry Counter Sync Daemon");
    info!("Stats reporting enabled: {}", args.enable_stats);
    if args.enable_stats {
        info!("Stats reporting interval: {} seconds", args.stats_interval);
        info!("Detailed stats: {}", args.detailed_stats);
        info!("Max stats per report: {}", args.max_stats_per_report);
    }

    // Create communication channels between actors
    let (_command_sender, command_receiver) = channel(1);
    let (socket_sender, socket_receiver) = channel(1);
    let (ipfix_template_sender, ipfix_template_receiver) = channel(1);
    let (saistats_sender, saistats_receiver) = channel(100); // Increased buffer for stats

    // Get netlink family and group configuration from SONiC constants
    let (family, group) = get_genl_family_group();
    info!("Using netlink family: '{}', group: '{}'", family, group);

    // Initialize and configure actors
    let mut netlink = NetlinkActor::new(family.as_str(), group.as_str(), command_receiver);
    netlink.add_recipient(socket_sender);
    
    let mut ipfix = IpfixActor::new(ipfix_template_receiver, socket_receiver);
    ipfix.add_recipient(saistats_sender);

    // Initialize SwssActor to monitor SONiC orchestrator messages
    let swss = match SwssActor::new(ipfix_template_sender) {
        Ok(actor) => actor,
        Err(e) => {
            error!("Failed to initialize SwssActor: {}", e);
            return Err(e.into());
        }
    };

    // Configure stats reporter with settings from command line arguments
    let stats_reporter = if args.enable_stats {
        let reporter_config = StatsReporterConfig {
            interval: Duration::from_secs(args.stats_interval),
            detailed: args.detailed_stats,
            max_stats_per_report: if args.max_stats_per_report == 0 { 
                None 
            } else { 
                Some(args.max_stats_per_report as usize) 
            },
        };
        Some(StatsReporterActor::new(saistats_receiver, reporter_config, ConsoleWriter))
    } else {
        // Drop the receiver if stats reporting is disabled
        drop(saistats_receiver);
        None
    };

    info!("Starting actor tasks...");
    
    // Spawn actor tasks
    let netlink_handle = spawn(async move {
        info!("Netlink actor started");
        NetlinkActor::run(netlink).await;
        info!("Netlink actor terminated");
    });
    
    let ipfix_handle = spawn(async move {
        info!("IPFIX actor started");
        IpfixActor::run(ipfix).await;
        info!("IPFIX actor terminated");
    });

    let swss_handle = spawn(async move {
        info!("SWSS actor started");
        SwssActor::run(swss).await;
        info!("SWSS actor terminated");
    });

    // Only spawn stats reporter if enabled
    let reporter_handle = if let Some(stats_reporter) = stats_reporter {
        Some(spawn(async move {
            info!("Stats reporter actor started");
            StatsReporterActor::run(stats_reporter).await;
            info!("Stats reporter actor terminated");
        }))
    } else {
        info!("Stats reporting disabled - not starting stats reporter actor");
        None
    };

    // Wait for all actors to complete and handle any errors
    let netlink_result = netlink_handle.await;
    let ipfix_result = ipfix_handle.await;
    let swss_result = swss_handle.await;
    let reporter_result = if let Some(handle) = reporter_handle {
        Some(handle.await)
    } else {
        None
    };

    // Handle results based on whether stats reporter was enabled
    if let Some(reporter_result) = reporter_result {
        match (netlink_result, ipfix_result, swss_result, reporter_result) {
            (Ok(()), Ok(()), Ok(()), Ok(())) => {
                info!("All actors completed successfully");
                Ok(())
            }
            (Err(e), _, _, _) => {
                error!("Netlink actor failed: {:?}", e);
                Err(e.into())
            }
            (_, Err(e), _, _) => {
                error!("IPFIX actor failed: {:?}", e);
                Err(e.into())
            }
            (_, _, Err(e), _) => {
                error!("SWSS actor failed: {:?}", e);
                Err(e.into())
            }
            (_, _, _, Err(e)) => {
                error!("Stats reporter actor failed: {:?}", e);
                Err(e.into())
            }
        }
    } else {
        match (netlink_result, ipfix_result, swss_result) {
            (Ok(()), Ok(()), Ok(())) => {
                info!("All actors completed successfully (stats reporting disabled)");
                Ok(())
            }
            (Err(e), _, _) => {
                error!("Netlink actor failed: {:?}", e);
                Err(e.into())
            }
            (_, Err(e), _) => {
                error!("IPFIX actor failed: {:?}", e);
                Err(e.into())
            }
            (_, _, Err(e)) => {
                error!("SWSS actor failed: {:?}", e);
                Err(e.into())
            }
        }
    }
}
