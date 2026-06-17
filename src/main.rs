use clap::Parser;
use hft_orderbook_gateway::network::{MulticastReceiver, TcpBroadcastServer};
use hft_orderbook_gateway::orderbook::OrderBook;
use hft_orderbook_gateway::protocol::{ItchParser, Side, ItchEvent};
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
    info!("Starting HFT OrderBook Gateway v{}", env!("CARGO_PKG_VERSION"));
    info!("Configuration: {:?}", args);

    let mc_group: Ipv4Addr = args.multicast_group.parse()?;
    let mc_iface: Ipv4Addr = args.multicast_interface.parse()?;
    let tcp_bind: SocketAddr = format!("0.0.0.0:{}", args.tcp_port).parse()?;

    let order_book = Arc::new(std::sync::Mutex::new(OrderBook::new()));
    let tcp_server = Arc::new(TcpBroadcastServer::new(tcp_bind)?);

    let total_packets = Arc::new(AtomicU64::new(0));
    let total_events = Arc::new(AtomicU64::new(0));
    let total_bbo_updates = Arc::new(AtomicU64::new(0));
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));

    {
        let running = running.clone();
        let total_packets = total_packets.clone();
        let total_events = total_events.clone();
        let total_bbo_updates = total_bbo_updates.clone();

        thread::Builder::new()
            .name("stats-monitor".to_string())
            .spawn(move || {
                while running.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_secs(1));
                    let pkts = total_packets.swap(0, Ordering::Relaxed);
                    let evts = total_events.swap(0, Ordering::Relaxed);
                    let bbos = total_bbo_updates.swap(0, Ordering::Relaxed);
                    if pkts > 0 || evts > 0 {
                        info!(
                            "Stats: {} packets/sec, {} events/sec, {} BBO updates/sec",
                            pkts, evts, bbos
                        );
                    }
                }
            })?;
    }

    if args.enable_test_generator {
        info!("Test data generator enabled");
        let order_book = order_book.clone();
        let tcp_server = tcp_server.clone();
        let total_events = total_events.clone();
        let total_bbo_updates = total_bbo_updates.clone();
        let running = running.clone();

        thread::Builder::new()
            .name("test-generator".to_string())
            .spawn(move || {
                run_test_generator(
                    order_book,
                    tcp_server,
                    total_events,
                    total_bbo_updates,
                    running,
                );
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

        let order_book = order_book.clone();
        let tcp_server = tcp_server.clone();
        let total_packets = total_packets.clone();
        let total_events = total_events.clone();
        let total_bbo_updates = total_bbo_updates.clone();
        let running = running.clone();

        thread::Builder::new()
            .name("multicast-consumer".to_string())
            .spawn(move || {
                run_multicast_consumer(
                    receiver,
                    order_book,
                    tcp_server,
                    total_packets,
                    total_events,
                    total_bbo_updates,
                    running,
                );
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

fn run_multicast_consumer(
    mut receiver: MulticastReceiver,
    order_book: Arc<std::sync::Mutex<OrderBook>>,
    tcp_server: Arc<TcpBroadcastServer>,
    total_packets: Arc<AtomicU64>,
    total_events: Arc<AtomicU64>,
    total_bbo_updates: Arc<AtomicU64>,
    running: Arc<std::sync::atomic::AtomicBool>,
) {
    let parser = ItchParser::new();

    while running.load(Ordering::Relaxed) {
        match receiver.receive() {
            Ok(packet) => {
                total_packets.fetch_add(1, Ordering::Relaxed);
                let mut events_count = 0u64;
                let mut bbo_count = 0u64;

                {
                    let mut ob = order_book.lock().unwrap();
                    for result in parser.parse_multicast_packet(packet) {
                        match result {
                            Ok(event) => {
                                events_count += 1;
                                if let Some(bbo) = ob.apply_event(&event) {
                                    tcp_server.broadcast_bbo(&bbo);
                                    bbo_count += 1;
                                }
                            }
                            Err(e) => {
                                warn!("Parse error: {}", e);
                            }
                        }
                    }
                }

                total_events.fetch_add(events_count, Ordering::Relaxed);
                total_bbo_updates.fetch_add(bbo_count, Ordering::Relaxed);
            }
            Err(e) => {
                error!("Multicast receive error: {}", e);
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

fn run_test_generator(
    order_book: Arc<std::sync::Mutex<OrderBook>>,
    tcp_server: Arc<TcpBroadcastServer>,
    total_events: Arc<AtomicU64>,
    total_bbo_updates: Arc<AtomicU64>,
    running: Arc<std::sync::atomic::AtomicBool>,
) {
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

        let mut events_count = 0u64;
        let mut bbo_count = 0u64;

        {
            let mut ob = order_book.lock().unwrap();
            for event in events.iter() {
                events_count += 1;
                if let Some(bbo) = ob.apply_event(event) {
                    tcp_server.broadcast_bbo(&bbo);
                    bbo_count += 1;
                }
            }
        }

        total_events.fetch_add(events_count, Ordering::Relaxed);
        total_bbo_updates.fetch_add(bbo_count, Ordering::Relaxed);

        if tick_count % 1000 == 0 {
            let elapsed = start.elapsed();
            if elapsed.as_secs() >= 300 {
                base_price = 1_500_000 + (tick_count / 100000) as u64 * 10000;
            }
        }

        if tcp_server.client_count() > 0 {
            thread::sleep(Duration::from_micros(500));
        } else {
            thread::sleep(Duration::from_millis(1));
        }
    }
}
