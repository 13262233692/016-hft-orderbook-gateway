use ahash::AHashMap;
use arrayvec::ArrayVec;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::protocol::{ItchEvent, Side};

pub const MAX_LEVELS: usize = 64;
pub const BBO_SERIALIZED_SIZE: usize = 216;

#[derive(Debug, Clone, Copy)]
pub struct PriceLevel {
    pub price: u64,
    pub volume: u64,
    pub order_count: u32,
}

impl PriceLevel {
    #[inline(always)]
    pub const fn zero() -> Self {
        Self {
            price: 0,
            volume: 0,
            order_count: 0,
        }
    }

    #[inline(always)]
    pub const fn new(price: u64, volume: u64, order_count: u32) -> Self {
        Self {
            price,
            volume,
            order_count,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BboUpdate {
    pub stock: u64,
    pub timestamp: u64,
    pub seq_num: u64,
    pub bid_price: u64,
    pub bid_volume: u64,
    pub ask_price: u64,
    pub ask_volume: u64,
    pub top_bids: ArrayVec<PriceLevel, 10>,
    pub top_asks: ArrayVec<PriceLevel, 10>,
}

impl BboUpdate {
    #[inline(always)]
    pub fn serialize(&self, buf: &mut [u8; BBO_SERIALIZED_SIZE]) {
        let mut pos = 0;
        buf[pos..pos + 8].copy_from_slice(&self.stock.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.timestamp.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.seq_num.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.bid_price.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.bid_volume.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.ask_price.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.ask_volume.to_le_bytes());
        pos += 8;

        for i in 0..5 {
            let level = self.top_bids.get(i).copied().unwrap_or(PriceLevel::zero());
            buf[pos..pos + 8].copy_from_slice(&level.price.to_le_bytes());
            pos += 8;
            buf[pos..pos + 8].copy_from_slice(&level.volume.to_le_bytes());
            pos += 8;
        }
        for i in 0..5 {
            let level = self.top_asks.get(i).copied().unwrap_or(PriceLevel::zero());
            buf[pos..pos + 8].copy_from_slice(&level.price.to_le_bytes());
            pos += 8;
            buf[pos..pos + 8].copy_from_slice(&level.volume.to_le_bytes());
            pos += 8;
        }
    }
}

#[derive(Debug, Clone)]
struct OrderInfo {
    side: Side,
    price: u64,
    shares: u32,
}

#[derive(Debug)]
struct BookSide {
    levels: BTreeMap<u64, PriceLevel>,
    side: Side,
}

impl BookSide {
    fn new(side: Side) -> Self {
        Self {
            levels: BTreeMap::new(),
            side,
        }
    }

    #[inline(always)]
    fn best(&self) -> Option<&PriceLevel> {
        match self.side {
            Side::Buy => self.levels.values().rev().next(),
            Side::Sell => self.levels.values().next(),
        }
    }

    #[inline(always)]
    fn add_order(&mut self, price: u64, shares: u32) {
        match self.levels.get_mut(&price) {
            Some(level) => {
                level.volume += shares as u64;
                level.order_count += 1;
            }
            None => {
                self.levels.insert(
                    price,
                    PriceLevel::new(price, shares as u64, 1),
                );
            }
        }
    }

    #[inline(always)]
    fn remove_shares(&mut self, price: u64, shares: u32) -> bool {
        if let Some(level) = self.levels.get_mut(&price) {
            level.volume = level.volume.saturating_sub(shares as u64);
            level.order_count = level.order_count.saturating_sub(1);
            if level.volume == 0 {
                self.levels.remove(&price);
                return true;
            }
        }
        false
    }

    #[inline(always)]
    fn delete_all_at_price(&mut self, price: u64, shares: u32) {
        if let Some(level) = self.levels.get_mut(&price) {
            level.volume = level.volume.saturating_sub(shares as u64);
            level.order_count = level.order_count.saturating_sub(1);
            if level.volume == 0 {
                self.levels.remove(&price);
            }
        }
    }

    #[inline(always)]
    fn top_n<const N: usize>(&self) -> ArrayVec<PriceLevel, N> {
        let mut result = ArrayVec::new();
        match self.side {
            Side::Buy => {
                for level in self.levels.values().rev().take(N) {
                    result.push(*level);
                }
            }
            Side::Sell => {
                for level in self.levels.values().take(N) {
                    result.push(*level);
                }
            }
        }
        result
    }
}

#[derive(Debug)]
pub struct SingleOrderBook {
    stock: u64,
    bids: BookSide,
    asks: BookSide,
    orders: AHashMap<u64, OrderInfo>,
    last_bbo_bid: u64,
    last_bbo_ask: u64,
    last_bbo_bid_vol: u64,
    last_bbo_ask_vol: u64,
    seq_num: AtomicU64,
}

impl SingleOrderBook {
    pub fn new(stock: u64) -> Self {
        Self {
            stock,
            bids: BookSide::new(Side::Buy),
            asks: BookSide::new(Side::Sell),
            orders: AHashMap::with_capacity(65536),
            last_bbo_bid: 0,
            last_bbo_ask: 0,
            last_bbo_bid_vol: 0,
            last_bbo_ask_vol: 0,
            seq_num: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn stock(&self) -> u64 {
        self.stock
    }

    #[inline(always)]
    fn bbo_changed(&self) -> bool {
        let bid = self.bids.best();
        let ask = self.asks.best();
        let cur_bid = bid.map(|l| l.price).unwrap_or(0);
        let cur_ask = ask.map(|l| l.price).unwrap_or(0);
        let cur_bid_vol = bid.map(|l| l.volume).unwrap_or(0);
        let cur_ask_vol = ask.map(|l| l.volume).unwrap_or(0);

        cur_bid != self.last_bbo_bid
            || cur_ask != self.last_bbo_ask
            || cur_bid_vol != self.last_bbo_bid_vol
            || cur_ask_vol != self.last_bbo_ask_vol
    }

    #[inline(always)]
    fn update_last_bbo(&mut self) {
        let bid = self.bids.best();
        let ask = self.asks.best();
        self.last_bbo_bid = bid.map(|l| l.price).unwrap_or(0);
        self.last_bbo_ask = ask.map(|l| l.price).unwrap_or(0);
        self.last_bbo_bid_vol = bid.map(|l| l.volume).unwrap_or(0);
        self.last_bbo_ask_vol = ask.map(|l| l.volume).unwrap_or(0);
    }

    #[inline(always)]
    fn create_bbo_update(&self, timestamp: u64) -> Option<BboUpdate> {
        if !self.bbo_changed() {
            return None;
        }

        let bid = self.bids.best();
        let ask = self.asks.best();

        Some(BboUpdate {
            stock: self.stock,
            timestamp,
            seq_num: self.seq_num.fetch_add(1, Ordering::Relaxed),
            bid_price: bid.map(|l| l.price).unwrap_or(0),
            bid_volume: bid.map(|l| l.volume).unwrap_or(0),
            ask_price: ask.map(|l| l.price).unwrap_or(0),
            ask_volume: ask.map(|l| l.volume).unwrap_or(0),
            top_bids: self.bids.top_n::<10>(),
            top_asks: self.asks.top_n::<10>(),
        })
    }

    #[inline(always)]
    pub fn apply_event(&mut self, event: &ItchEvent) -> Option<BboUpdate> {
        let ts = event.timestamp();
        match event {
            ItchEvent::AddOrder {
                order_ref,
                side,
                shares,
                stock,
                price,
                ..
            }
            | ItchEvent::AddOrderMpid {
                order_ref,
                side,
                shares,
                stock,
                price,
                ..
            } => {
                self.orders.insert(
                    *order_ref,
                    OrderInfo {
                        side: *side,
                        price: *price,
                        shares: *shares,
                    },
                );
                match side {
                    Side::Buy => self.bids.add_order(*price, *shares),
                    Side::Sell => self.asks.add_order(*price, *shares),
                }
                let _ = stock;
            }

            ItchEvent::OrderExecuted {
                order_ref,
                shares,
                ..
            }
            | ItchEvent::OrderExecutedPrice {
                order_ref,
                shares,
                ..
            } => {
                if let Some(info) = self.orders.get_mut(order_ref) {
                    let executed_shares = (*shares).min(info.shares);
                    info.shares -= executed_shares;
                    let side = info.side;
                    let price = info.price;
                    match side {
                        Side::Buy => {
                            self.bids.remove_shares(price, executed_shares);
                        }
                        Side::Sell => {
                            self.asks.remove_shares(price, executed_shares);
                        }
                    }
                    if info.shares == 0 {
                        self.orders.remove(order_ref);
                    }
                }
            }

            ItchEvent::OrderCancel {
                order_ref,
                shares,
                ..
            } => {
                if let Some(info) = self.orders.get_mut(order_ref) {
                    let cancel_shares = (*shares).min(info.shares);
                    info.shares -= cancel_shares;
                    let side = info.side;
                    let price = info.price;
                    match side {
                        Side::Buy => {
                            self.bids.remove_shares(price, cancel_shares);
                        }
                        Side::Sell => {
                            self.asks.remove_shares(price, cancel_shares);
                        }
                    }
                    if info.shares == 0 {
                        self.orders.remove(order_ref);
                    }
                }
            }

            ItchEvent::OrderDelete { order_ref, .. } => {
                if let Some(info) = self.orders.remove(order_ref) {
                    match info.side {
                        Side::Buy => {
                            self.bids.delete_all_at_price(info.price, info.shares);
                        }
                        Side::Sell => {
                            self.asks.delete_all_at_price(info.price, info.shares);
                        }
                    }
                }
            }

            ItchEvent::OrderReplace {
                order_ref,
                new_order_ref,
                shares,
                price,
                ..
            } => {
                if let Some(old_info) = self.orders.remove(order_ref) {
                    match old_info.side {
                        Side::Buy => {
                            self.bids.delete_all_at_price(old_info.price, old_info.shares);
                            self.bids.add_order(*price, *shares);
                        }
                        Side::Sell => {
                            self.asks.delete_all_at_price(old_info.price, old_info.shares);
                            self.asks.add_order(*price, *shares);
                        }
                    }
                    self.orders.insert(
                        *new_order_ref,
                        OrderInfo {
                            side: old_info.side,
                            price: *price,
                            shares: *shares,
                        },
                    );
                }
            }

            _ => {}
        }

        let bbo = self.create_bbo_update(ts);
        if bbo.is_some() {
            self.update_last_bbo();
        }
        bbo
    }

    #[inline(always)]
    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids.best()
    }

    #[inline(always)]
    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks.best()
    }

    #[inline(always)]
    pub fn top_bids<const N: usize>(&self) -> ArrayVec<PriceLevel, N> {
        self.bids.top_n()
    }

    #[inline(always)]
    pub fn top_asks<const N: usize>(&self) -> ArrayVec<PriceLevel, N> {
        self.asks.top_n()
    }
}

pub struct OrderBook {
    books: AHashMap<u64, SingleOrderBook>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            books: AHashMap::with_capacity(1024),
        }
    }

    #[inline(always)]
    pub fn get_or_create(&mut self, stock: u64) -> &mut SingleOrderBook {
        self.books
            .entry(stock)
            .or_insert_with(|| SingleOrderBook::new(stock))
    }

    #[inline(always)]
    pub fn get(&self, stock: u64) -> Option<&SingleOrderBook> {
        self.books.get(&stock)
    }

    #[inline(always)]
    pub fn apply_event(&mut self, event: &ItchEvent) -> Option<BboUpdate> {
        match event {
            ItchEvent::AddOrder { stock, .. }
            | ItchEvent::AddOrderMpid { stock, .. } => {
                let book = self.get_or_create(*stock);
                book.apply_event(event)
            }
            ItchEvent::OrderExecuted { order_ref, .. }
            | ItchEvent::OrderExecutedPrice { order_ref, .. }
            | ItchEvent::OrderCancel { order_ref, .. }
            | ItchEvent::OrderDelete { order_ref, .. } => {
                let stock = self.find_stock_for_order(*order_ref);
                if let Some(stock) = stock {
                    let book = self.get_or_create(stock);
                    book.apply_event(event)
                } else {
                    let mut result = None;
                    for book in self.books.values_mut() {
                        if book.orders.contains_key(order_ref) {
                            result = book.apply_event(event);
                            break;
                        }
                    }
                    result
                }
            }
            ItchEvent::OrderReplace { order_ref, .. } => {
                let stock = self.find_stock_for_order(*order_ref);
                if let Some(stock) = stock {
                    let book = self.get_or_create(stock);
                    book.apply_event(event)
                } else {
                    let mut result = None;
                    for book in self.books.values_mut() {
                        if book.orders.contains_key(order_ref) {
                            result = book.apply_event(event);
                            break;
                        }
                    }
                    result
                }
            }
            _ => None,
        }
    }

    fn find_stock_for_order(&self, order_ref: u64) -> Option<u64> {
        for book in self.books.values() {
            if book.orders.contains_key(&order_ref) {
                return Some(book.stock);
            }
        }
        None
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.books.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.books.is_empty()
    }

    pub fn iter_books(&self) -> impl Iterator<Item = &SingleOrderBook> {
        self.books.values()
    }
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::ItchEvent;

    const TEST_STOCK: u64 = 0x4141504C20202020;

    fn add_order_event(order_ref: u64, side: Side, shares: u32, price: u64) -> ItchEvent {
        ItchEvent::AddOrder {
            timestamp: 1,
            order_ref,
            side,
            shares,
            stock: TEST_STOCK,
            price,
        }
    }

    fn delete_order_event(order_ref: u64) -> ItchEvent {
        ItchEvent::OrderDelete {
            timestamp: 2,
            order_ref,
        }
    }

    fn cancel_order_event(order_ref: u64, shares: u32) -> ItchEvent {
        ItchEvent::OrderCancel {
            timestamp: 2,
            order_ref,
            shares,
        }
    }

    fn replace_order_event(
        order_ref: u64,
        new_order_ref: u64,
        shares: u32,
        price: u64,
    ) -> ItchEvent {
        ItchEvent::OrderReplace {
            timestamp: 3,
            order_ref,
            new_order_ref,
            shares,
            price,
        }
    }

    #[test]
    fn test_single_book_add_and_bbo() {
        let mut book = SingleOrderBook::new(TEST_STOCK);

        let bbo = book.apply_event(&add_order_event(1, Side::Buy, 100, 1000000));
        assert!(bbo.is_some());
        let bbo = bbo.unwrap();
        assert_eq!(bbo.bid_price, 1000000);
        assert_eq!(bbo.bid_volume, 100);
        assert_eq!(bbo.ask_price, 0);

        let bbo = book.apply_event(&add_order_event(2, Side::Sell, 200, 1100000));
        assert!(bbo.is_some());
        let bbo = bbo.unwrap();
        assert_eq!(bbo.bid_price, 1000000);
        assert_eq!(bbo.ask_price, 1100000);
        assert_eq!(bbo.ask_volume, 200);

        let bbo = book.apply_event(&add_order_event(3, Side::Buy, 50, 1000000));
        assert!(bbo.is_some());
        let bbo = bbo.unwrap();
        assert_eq!(bbo.bid_volume, 150);
    }

    #[test]
    fn test_order_delete() {
        let mut book = SingleOrderBook::new(TEST_STOCK);

        book.apply_event(&add_order_event(1, Side::Buy, 100, 1000000));
        book.apply_event(&add_order_event(2, Side::Buy, 200, 1000000));

        let best_bid = book.best_bid().unwrap();
        assert_eq!(best_bid.volume, 300);
        assert_eq!(best_bid.order_count, 2);

        let bbo = book.apply_event(&delete_order_event(1));
        assert!(bbo.is_some());
        let best_bid = book.best_bid().unwrap();
        assert_eq!(best_bid.volume, 200);
        assert_eq!(best_bid.order_count, 1);
    }

    #[test]
    fn test_order_cancel() {
        let mut book = SingleOrderBook::new(TEST_STOCK);

        book.apply_event(&add_order_event(1, Side::Buy, 100, 1000000));
        let bbo = book.apply_event(&cancel_order_event(1, 30));
        assert!(bbo.is_some());

        let best_bid = book.best_bid().unwrap();
        assert_eq!(best_bid.volume, 70);
        assert_eq!(best_bid.order_count, 0);
    }

    #[test]
    fn test_order_replace() {
        let mut book = SingleOrderBook::new(TEST_STOCK);

        book.apply_event(&add_order_event(1, Side::Buy, 100, 1000000));
        let bbo = book.apply_event(&replace_order_event(1, 2, 150, 1050000));
        assert!(bbo.is_some());

        let best_bid = book.best_bid().unwrap();
        assert_eq!(best_bid.price, 1050000);
        assert_eq!(best_bid.volume, 150);
    }

    #[test]
    fn test_price_level_consolidation() {
        let mut book = SingleOrderBook::new(TEST_STOCK);

        for i in 0..5 {
            book.apply_event(&add_order_event(100 + i, Side::Buy, 100, 1000000));
        }
        let best_bid = book.best_bid().unwrap();
        assert_eq!(best_bid.volume, 500);
        assert_eq!(best_bid.order_count, 5);
    }

    #[test]
    fn test_top_levels() {
        let mut book = SingleOrderBook::new(TEST_STOCK);

        book.apply_event(&add_order_event(1, Side::Buy, 100, 1000000));
        book.apply_event(&add_order_event(2, Side::Buy, 200, 990000));
        book.apply_event(&add_order_event(3, Side::Buy, 300, 980000));
        book.apply_event(&add_order_event(4, Side::Sell, 150, 1010000));
        book.apply_event(&add_order_event(5, Side::Sell, 250, 1020000));

        let bids: ArrayVec<PriceLevel, 10> = book.top_bids::<10>();
        let asks: ArrayVec<PriceLevel, 10> = book.top_asks::<10>();

        assert_eq!(bids.len(), 3);
        assert_eq!(bids[0].price, 1000000);
        assert_eq!(bids[1].price, 990000);
        assert_eq!(bids[2].price, 980000);

        assert_eq!(asks.len(), 2);
        assert_eq!(asks[0].price, 1010000);
        assert_eq!(asks[1].price, 1020000);
    }

    #[test]
    fn test_multistock_orderbook() {
        let mut ob = OrderBook::new();
        const STOCK_A: u64 = 0x4141504C20202020;
        const STOCK_B: u64 = 0x4D53465420202020;

        ob.apply_event(&ItchEvent::AddOrder {
            timestamp: 1,
            order_ref: 1,
            side: Side::Buy,
            shares: 100,
            stock: STOCK_A,
            price: 1000000,
        });

        ob.apply_event(&ItchEvent::AddOrder {
            timestamp: 1,
            order_ref: 2,
            side: Side::Buy,
            shares: 200,
            stock: STOCK_B,
            price: 2000000,
        });

        assert_eq!(ob.len(), 2);

        let book_a = ob.get(STOCK_A).unwrap();
        let book_b = ob.get(STOCK_B).unwrap();

        assert_eq!(book_a.best_bid().unwrap().price, 1000000);
        assert_eq!(book_b.best_bid().unwrap().price, 2000000);
    }

    #[test]
    fn test_bbo_serialization() {
        let update = BboUpdate {
            stock: 123,
            timestamp: 456,
            seq_num: 789,
            bid_price: 1000000,
            bid_volume: 500,
            ask_price: 1010000,
            ask_volume: 300,
            top_bids: ArrayVec::new(),
            top_asks: ArrayVec::new(),
        };

        let mut buf = [0u8; BBO_SERIALIZED_SIZE];
        update.serialize(&mut buf);

        assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 123);
        assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), 456);
        assert_eq!(u64::from_le_bytes(buf[16..24].try_into().unwrap()), 789);
        assert_eq!(u64::from_le_bytes(buf[24..32].try_into().unwrap()), 1000000);
        assert_eq!(u64::from_le_bytes(buf[32..40].try_into().unwrap()), 500);
        assert_eq!(u64::from_le_bytes(buf[40..48].try_into().unwrap()), 1010000);
        assert_eq!(u64::from_le_bytes(buf[48..56].try_into().unwrap()), 300);
    }
}
