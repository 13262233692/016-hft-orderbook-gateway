use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use hft_orderbook_gateway::orderbook::SingleOrderBook;
use hft_orderbook_gateway::protocol::{ItchEvent, Side};

const TEST_STOCK: u64 = 0x4141504C20202020;

fn create_events(n: usize) -> Vec<ItchEvent> {
    let mut events = Vec::with_capacity(n * 3);
    let mut order_ref: u64 = 1;
    let mut ts: u64 = 1;

    for i in 0..n {
        let base_price = 1_500_000 + (i as u64 % 100) * 1000;

        events.push(ItchEvent::AddOrder {
            timestamp: ts,
            order_ref,
            side: Side::Buy,
            shares: 100 + (i % 500) as u32,
            stock: TEST_STOCK,
            price: base_price - 1000,
        });
        order_ref += 1;
        ts += 1;

        events.push(ItchEvent::AddOrder {
            timestamp: ts,
            order_ref,
            side: Side::Sell,
            shares: 150 + (i % 400) as u32,
            stock: TEST_STOCK,
            price: base_price + 1000,
        });
        order_ref += 1;
        ts += 1;

        if i > 10 && i % 3 == 0 {
            events.push(ItchEvent::OrderDelete {
                timestamp: ts,
                order_ref: order_ref - 20,
            });
            ts += 1;
        }

        if i > 5 && i % 5 == 0 {
            events.push(ItchEvent::OrderCancel {
                timestamp: ts,
                order_ref: order_ref - 15,
                shares: 50,
            });
            ts += 1;
        }
    }

    events
}

fn criterion_benchmark(c: &mut Criterion) {
    let events = create_events(10_000);

    let mut group = c.benchmark_group("orderbook_throughput");
    group.throughput(Throughput::Elements(events.len() as u64));

    group.bench_function("apply_events_10k", |b| {
        b.iter(|| {
            let mut book = SingleOrderBook::new(TEST_STOCK);
            let mut bbo_count = 0;
            for event in &events {
                if book.apply_event(event).is_some() {
                    bbo_count += 1;
                }
            }
            bbo_count
        });
    });

    group.finish();

    let mut group2 = c.benchmark_group("single_operations");

    group2.bench_function("add_order", |b| {
        let mut book = SingleOrderBook::new(TEST_STOCK);
        let mut order_ref = 1u64;
        b.iter(|| {
            let event = ItchEvent::AddOrder {
                timestamp: 1,
                order_ref,
                side: Side::Buy,
                shares: 100,
                stock: TEST_STOCK,
                price: 1_500_000,
            };
            order_ref += 1;
            book.apply_event(&event)
        });
    });

    group2.bench_function("add_order_at_many_prices", |b| {
        let mut book = SingleOrderBook::new(TEST_STOCK);
        let mut order_ref = 1u64;
        let mut price_idx = 0u64;
        b.iter(|| {
            let price = 1_000_000 + price_idx * 100;
            price_idx = (price_idx + 1) % 1000;
            let event = ItchEvent::AddOrder {
                timestamp: 1,
                order_ref,
                side: if price_idx % 2 == 0 { Side::Buy } else { Side::Sell },
                shares: 100,
                stock: TEST_STOCK,
                price,
            };
            order_ref += 1;
            book.apply_event(&event)
        });
    });

    group2.finish();

    let mut group3 = c.benchmark_group("bbo_operations");

    group3.bench_function("bbo_read_after_1000_orders", |b| {
        let mut book = SingleOrderBook::new(TEST_STOCK);
        for i in 0..1000 {
            let price = 1_500_000 + (i as u64 % 50) * 1000 - 25 * 1000;
            let event = ItchEvent::AddOrder {
                timestamp: i as u64,
                order_ref: i as u64,
                side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
                shares: 100,
                stock: TEST_STOCK,
                price: if i % 2 == 0 { 1_500_000 - (i as u64 % 10) * 1000 } else { 1_500_000 + (i as u64 % 10) * 1000 + 1000 },
            };
            book.apply_event(&event);
        }
        b.iter(|| (book.best_bid().map(|l| l.price), book.best_ask().map(|l| l.price)));
    });

    group3.bench_function("top_10_levels", |b| {
        let mut book = SingleOrderBook::new(TEST_STOCK);
        for i in 0..200 {
            let event = ItchEvent::AddOrder {
                timestamp: i as u64,
                order_ref: i as u64,
                side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
                shares: 100 + (i % 100) as u32,
                stock: TEST_STOCK,
                price: if i % 2 == 0 { 1_500_000 - (i as u64 / 2 % 20) * 100 } else { 1_500_100 + (i as u64 / 2 % 20) * 100 },
            };
            book.apply_event(&event);
        }
        b.iter(|| {
            let bids = book.top_bids::<10>();
            let asks = book.top_asks::<10>();
            (bids.len(), asks.len())
        });
    });

    group3.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
