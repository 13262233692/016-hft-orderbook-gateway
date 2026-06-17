use clap::Parser;
use hft_orderbook_gateway::network::{MulticastReceiver, TcpBroadcastServer};
use hft_orderbook_gateway::orderbook::OrderBook;
use hft_orderbook_gateway::protocol::{ItchEvent, ItchParser, Side};
use hft_orderbook_gateway::pipeline::Pipeline;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "233.222.7.1")]
    multicast_group: String,

    #[arg(long, default_value_t = 15001)]
    multicast_port: u16,

    #[arg(long, default_value = "0.0.0.0")]
    multicast_interface: String,

    #[arg(long, default_value = "838383")]
    tcp_port: String,

    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    socket_buffer_size: usize,

    #[arg(long, default_value_t = false)]
    enable_test_generator: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .with_thread_ids(true)
        .init();

    let args = Args::parse();
    info!(
        "Starting HFT OrderBook Gateway v{}",
        env!("CARGO_PKG_VERSION")
    );
    info!("Configuration: {:?}", args);

    let mc_group: Ipv4Addr = args.multicast_group.parse()?;
    let mc_iface: Ipv4Addr = args.multicast_interface.parse()?;
    let tcp_bind: SocketAddr = format!("0.0.0.0:{}", args.tcp_port).parse()?;

    let pipeline = Arc::new(Pipeline::new());
    let tcp_server = Arc::new(TcpBroadcastServer::new(tcp_bind)?);

    let total_events = Arc::new(AtomicU64::new(0));
    let total_bbo_updates = Arc::new(AtomicU64::new(0));
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));

    {
        let running = running.clone();
        let total_events = total_events.clone();
        let total_bbo_updates = total_bbo_updates.clone();

        thread::Builder::new()
            .name("stats-monitor".to_string())
            .spawn(move || {
                while running.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_secs(1));
                    let evts = total_events.swap(0, Ordering::Relaxed);
                    let bbos = total_bbo_updates.swap(0, Ordering::Relaxed);
                    if evts > 0 || bbos > 0 {
                        info!(
                            "Stats: {} events/sec, {} BBO updates/sec",
                            evts, bbos
                        );
                    }
                }
            })?;
    }

    {
        let pipeline = pipeline.clone();
        let tcp_server = tcp_server.clone();
        let running = running.clone();
        let total_events = total_events.clone();
        let total_bbo_updates = total_bbo_updates.clone();

        thread::Builder::new()
            .name("orderbook-writer".to_string())
            .spawn(move || {
                run_orderbook_writer(
                    pipeline,
                    tcp_server,
                    running,
                    total_events,
                    total_bbo_updates,
                );
            })?;
    }

    if args.enable_test_generator {
        info!("Test data generator enabled (single-writer pipeline)");
        let pipeline = pipeline.clone();
        let running = running.clone();

        thread::Builder::new()
            .name("test-generator".to_string())
            .spawn(move || {
                run_test_generator(pipeline, running);
            })?;
    } else {
        let receiver = MulticastReceiver::new(
            mc_group,
            args.multicast_port,
            mc_iface,
            args.socket_buffer_size,
        )?;
        info!(
            "Multicast receiver joined {}:{} on interface {}",
            mc_group, args.multicast_port, mc_iface
        );

        let pipeline = pipeline.clone();
        let running = running.clone();

        thread::Builder::new()
            .name("multicast-consumer".to_string())
            .spawn(move || {
                run_multicast_consumer(receiver, pipeline, running);
            })?;
    }

    info!("Gateway is running. Press Ctrl+C to shutdown.");

    let running_clone = running.clone();
    ctrlc::set_handler(move || {
        warn!("Received shutdown signal, stopping gracefully...");
        running_clone.store(false, Ordering::Relaxed);
    })?;

    while running.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }

    info!("Shutting down gateway...");
    tcp_server.shutdown();
    info!("Gateway stopped cleanly. Goodbye!");

    Ok(())
}

fn run_orderbook_writer(
    pipeline: Arc<Pipeline>,
    tcp_server: Arc<TcpBroadcastServer>,
    running: Arc<std::sync::atomic::AtomicBool>,
    total_events: Arc<AtomicU64>,
    total_bbo_updates: Arc<AtomicU64>,
) {
    let mut order_book = OrderBook::new();
    const BATCH_SIZE: usize = 1024;
    let mut events_count: u64;
    let mut bbo_count: u64;

    while running.load(Ordering::Relaxed) {
        events_count = 0;
        bbo_count = 0;

        for _ in 0..BATCH_SIZE {
            match pipeline.event_tx.pop() {
                Some(event) => {
                    events_count += 1;
                    if let Some(bbo) = order_book.apply_event(&event) {
                        tcp_server.enqueue_bbo(bbo);
                        bbo_count += 1;
                    }
                }
                None => break,
            }
        }

        if events_count > 0 {
            total_events.fetch_add(events_count, Ordering::Relaxed);
            total_bbo_updates.fetch_add(bbo_count, Ordering::Relaxed);
            if !order_book.all_consistent() {
                error!("CROSSED BOOK DETECTED inside single-writer thread!");
            }
        } else {
            thread::sleep(Duration::from_micros(20));
        }
    }
}

fn run_multicast_consumer(
    mut receiver: MulticastReceiver,
    pipeline: Arc<Pipeline>,
    running: Arc<std::sync::atomic::AtomicBool>,
) {
    let parser = ItchParser::new();

    while running.load(Ordering::Relaxed) {
        match receiver.receive() {
            Ok(packet) => {
                for result in parser.parse_multicast_packet(packet) {
                    match result {
                        Ok(event) => {
                            let _ = pipeline.push_event(event);
                        }
                        Err(e) => {
                            warn!("Parse error: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Multicast receive error: {}", e);
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

fn run_test_generator(pipeline: Arc<Pipeline>, running: Arc<std::sync::atomic::AtomicBool>) {
    const STOCK_AAPL: u64 = 0x4141504C20202020;
    let mut order_ref: u64 = 1;
    let mut base_price: u64 = 1_500_000;
    let start = Instant::now();
    let mut tick_count = 0u64;

    while running.load(Ordering::Relaxed) {
        tick_count += 1;

        let drift = ((tick_count as i64 % 100) - 50) as u64;
        let mid_price = base_price + drift * 1000;

        let events: [ItchEvent; 6] = [
            ItchEvent::AddOrder {
                timestamp: tick_count,
                order_ref: {
                    order_ref += 1;
                    order_ref
                },
                side: Side::Buy,
                shares: 100 + (tick_count % 500) as u32,
                stock: STOCK_AAPL,
                price: mid_price - 1000,
            },
            ItchEvent::AddOrder {
                timestamp: tick_count,
                order_ref: {
                    order_ref += 1;
                    order_ref
                },
                side: Side::Buy,
                shares: 200 + (tick_count % 300) as u32,
                stock: STOCK_AAPL,
                price: mid_price - 2000,
            },
            ItchEvent::AddOrder {
                timestamp: tick_count,
                order_ref: {
                    order_ref += 1;
                    order_ref
                },
                side: Side::Sell,
                shares: 150 + (tick_count % 400) as u32,
                stock: STOCK_AAPL,
                price: mid_price + 1000,
            },
            ItchEvent::AddOrder {
                timestamp: tick_count,
                order_ref: {
                    order_ref += 1;
                    order_ref
                },
                side: Side::Sell,
                shares: 250 + (tick_count % 200) as u32,
                stock: STOCK_AAPL,
                price: mid_price + 2000,
            },
            ItchEvent::OrderDelete {
                timestamp: tick_count,
                order_ref: order_ref.saturating_sub(48),
            },
            ItchEvent::OrderDelete {
                timestamp: tick_count,
                order_ref: order_ref.saturating_sub(47),
            },
        ];

        for event in events.iter() {
            let _ = pipeline.push_event(event.clone());
        }

        if tick_count % 1000 == 0 {
            let elapsed = start.elapsed();
            if elapsed.as_secs() >= 300 {
                base_price = 1_500_000 + (tick_count / 100000) as u64 * 10000;
            }
        }

        thread::sleep(Duration::from_micros(50));
    }
}
