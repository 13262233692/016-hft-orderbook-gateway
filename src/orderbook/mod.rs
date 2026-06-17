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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BboSnapshot {
    bid_price: u64,
    bid_volume: u64,
    ask_price: u64,
    ask_volume: u64,
}

impl BboSnapshot {
    #[inline(always)]
    const fn zero() -> Self {
        Self {
            bid_price: 0,
            bid_volume: 0,
            ask_price: 0,
            ask_volume: 0,
        }
    }

    #[inline(always)]
    fn is_crossed(&self) -> bool {
        self.bid_price != 0 && self.ask_price != 0 && self.bid_price > self.ask_price
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
pub(crate) struct OrderInfo {
    side: Side,
    price: u64,
    shares: u32,
}

#[derive(Debug)]
pub(crate) struct BookSide {
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
    fn best_snapshot(&self) -> (u64, u64) {
        match self.side {
            Side::Buy => self
                .levels
                .values()
                .rev()
                .next()
                .map(|l| (l.price, l.volume))
                .unwrap_or((0, 0)),
            Side::Sell => self
                .levels
                .values()
                .next()
                .map(|l| (l.price, l.volume))
                .unwrap_or((0, 0)),
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
                self.levels
                    .insert(price, PriceLevel::new(price, shares as u64, 1));
            }
        }
    }

    #[inline(always)]
    fn remove_shares(&mut self, price: u64, shares: u32) {
        if let Some(level) = self.levels.get_mut(&price) {
            level.volume = level.volume.saturating_sub(shares as u64);
            level.order_count = level.order_count.saturating_sub(1);
            if level.volume == 0 {
                self.levels.remove(&price);
            }
        }
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
    fn replace_atomically(&mut self, old_price: u64, old_shares: u32, new_price: u64, new_shares: u32) {
        if old_price == new_price {
            if let Some(level) = self.levels.get_mut(&old_price) {
                level.volume = level.volume.saturating_sub(old_shares as u64) + new_shares as u64;
                if level.volume == 0 {
                    self.levels.remove(&old_price);
                }
            } else {
                self.levels
                    .insert(new_price, PriceLevel::new(new_price, new_shares as u64, 1));
            }
            return;
        }

        let needs_insert_new = if let Some(level) = self.levels.get_mut(&old_price) {
            level.volume = level.volume.saturating_sub(old_shares as u64);
            level.order_count = level.order_count.saturating_sub(1);
            if level.volume == 0 {
                self.levels.remove(&old_price);
            }
            true
        } else {
            true
        };

        if needs_insert_new {
            match self.levels.get_mut(&new_price) {
                Some(level) => {
                    level.volume += new_shares as u64;
                    level.order_count += 1;
                }
                None => {
                    self.levels
                        .insert(new_price, PriceLevel::new(new_price, new_shares as u64, 1));
                }
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
    pub(crate) stock: u64,
    pub(crate) bids: BookSide,
    pub(crate) asks: BookSide,
    pub(crate) orders: AHashMap<u64, OrderInfo>,
    last_snapshot: BboSnapshot,
    seq_num: AtomicU64,
    generation: u64,
}

impl SingleOrderBook {
    pub fn new(stock: u64) -> Self {
        Self {
            stock,
            bids: BookSide::new(Side::Buy),
            asks: BookSide::new(Side::Sell),
            orders: AHashMap::with_capacity(65536),
            last_snapshot: BboSnapshot::zero(),
            seq_num: AtomicU64::new(0),
            generation: 0,
        }
    }

    #[inline(always)]
    pub fn stock(&self) -> u64 {
        self.stock
    }

    #[inline(always)]
    fn snapshot_bbo(&self) -> BboSnapshot {
        let (bid_price, bid_volume) = self.bids.best_snapshot();
        let (ask_price, ask_volume) = self.asks.best_snapshot();
        BboSnapshot {
            bid_price,
            bid_volume,
            ask_price,
            ask_volume,
        }
    }

    #[inline(always)]
    fn create_bbo_update(
        &self,
        timestamp: u64,
        snapshot: BboSnapshot,
    ) -> BboUpdate {
        BboUpdate {
            stock: self.stock,
            timestamp,
            seq_num: self.seq_num.fetch_add(1, Ordering::Relaxed),
            bid_price: snapshot.bid_price,
            bid_volume: snapshot.bid_volume,
            ask_price: snapshot.ask_price,
            ask_volume: snapshot.ask_volume,
            top_bids: self.bids.top_n::<10>(),
            top_asks: self.asks.top_n::<10>(),
        }
    }

    #[inline(always)]
    pub fn apply_event(&mut self, event: &ItchEvent) -> Option<BboUpdate> {
        let ts = event.timestamp();
        let mut touched = false;

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
                touched = true;
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
                    if executed_shares > 0 {
                        info.shares -= executed_shares;
                        let side = info.side;
                        let price = info.price;
                        match side {
                            Side::Buy => self.bids.remove_shares(price, executed_shares),
                            Side::Sell => self.asks.remove_shares(price, executed_shares),
                        }
                        touched = true;
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
                    if cancel_shares > 0 {
                        info.shares -= cancel_shares;
                        let side = info.side;
                        let price = info.price;
                        match side {
                            Side::Buy => self.bids.remove_shares(price, cancel_shares),
                            Side::Sell => self.asks.remove_shares(price, cancel_shares),
                        }
                        touched = true;
                    }
                    if info.shares == 0 {
                        self.orders.remove(order_ref);
                    }
                }
            }

            ItchEvent::OrderDelete { order_ref, .. } => {
                if let Some(info) = self.orders.remove(order_ref) {
                    match info.side {
                        Side::Buy => self.bids.delete_all_at_price(info.price, info.shares),
                        Side::Sell => self.asks.delete_all_at_price(info.price, info.shares),
                    }
                    touched = true;
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
                    let new_price = *price;
                    let new_shares = *shares;
                    let side = old_info.side;

                    match side {
                        Side::Buy => self
                            .bids
                            .replace_atomically(old_info.price, old_info.shares, new_price, new_shares),
                        Side::Sell => self
                            .asks
                            .replace_atomically(old_info.price, old_info.shares, new_price, new_shares),
                    }

                    self.orders.insert(
                        *new_order_ref,
                        OrderInfo {
                            side,
                            price: new_price,
                            shares: new_shares,
                        },
                    );
                    touched = true;
                }
            }

            _ => {}
        }

        if !touched {
            return None;
        }

        self.generation = self.generation.wrapping_add(1);

        let snap = self.snapshot_bbo();

        debug_assert!(
            !snap.is_crossed(),
            "Crossed book detected for stock={:#018X}: bid={} ask={} gen={}",
            self.stock,
            snap.bid_price,
            snap.ask_price,
            self.generation
        );

        if snap == self.last_snapshot {
            return None;
        }

        self.last_snapshot = snap;
        Some(self.create_bbo_update(ts, snap))
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

    #[inline(always)]
    pub fn is_consistent(&self) -> bool {
        !self.snapshot_bbo().is_crossed()
    }
}

pub struct OrderBook {
    books: AHashMap<u64, SingleOrderBook>,
    order_to_stock: AHashMap<u64, u64>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            books: AHashMap::with_capacity(1024),
            order_to_stock: AHashMap::with_capacity(65536),
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
            ItchEvent::AddOrder {
                order_ref, stock, ..
            }
            | ItchEvent::AddOrderMpid {
                order_ref, stock, ..
            } => {
                self.order_to_stock.insert(*order_ref, *stock);
                let book = self.get_or_create(*stock);
                book.apply_event(event)
            }

            ItchEvent::OrderReplace {
                order_ref,
                new_order_ref,
                ..
            } => {
                let maybe_stock = self.order_to_stock.get(order_ref).copied();
                if let Some(stock) = maybe_stock {
                    self.order_to_stock.remove(order_ref);
                    self.order_to_stock.insert(*new_order_ref, stock);
                    let book = self.get_or_create(stock);
                    book.apply_event(event)
                } else {
                    None
                }
            }

            ItchEvent::OrderExecuted { order_ref, .. }
            | ItchEvent::OrderExecutedPrice { order_ref, .. }
            | ItchEvent::OrderCancel { order_ref, .. } => {
                let maybe_stock = self.order_to_stock.get(order_ref).copied();
                if let Some(stock) = maybe_stock {
                    let (result, shares_remaining) = {
                        let book = self.get_or_create(stock);
                        let result = book.apply_event(event);
                        let shares_remaining = book
                            .orders
                            .get(order_ref)
                            .map(|info| info.shares)
                            .unwrap_or(0);
                        (result, shares_remaining)
                    };
                    if shares_remaining == 0 {
                        self.order_to_stock.remove(order_ref);
                    }
                    result
                } else {
                    None
                }
            }

            ItchEvent::OrderDelete { order_ref, .. } => {
                self.order_to_stock.remove(order_ref);
                let mut result = None;
                for book in self.books.values_mut() {
                    if book.orders.contains_key(order_ref) {
                        result = book.apply_event(event);
                        break;
                    }
                }
                result
            }

            _ => None,
        }
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

    #[inline(always)]
    pub fn all_consistent(&self) -> bool {
        self.books.values().all(|b| b.is_consistent())
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
    use std::sync::Arc;
    use std::thread;

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
        assert!(book.is_consistent());
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
        assert!(book.is_consistent());
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
        assert!(book.is_consistent());
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
        assert!(book.is_consistent());
    }

    #[test]
    fn test_order_replace_same_price() {
        let mut book = SingleOrderBook::new(TEST_STOCK);

        book.apply_event(&add_order_event(1, Side::Buy, 100, 1000000));
        book.apply_event(&add_order_event(2, Side::Buy, 100, 1000000));
        let bbo = book.apply_event(&replace_order_event(1, 3, 200, 1000000));
        assert!(bbo.is_some());

        let best_bid = book.best_bid().unwrap();
        assert_eq!(best_bid.price, 1000000);
        assert_eq!(best_bid.volume, 300);
        assert_eq!(best_bid.order_count, 2);
        assert!(book.is_consistent());
    }

    #[test]
    fn test_replace_storm_no_cross() {
        let mut book = SingleOrderBook::new(TEST_STOCK);

        for i in 0..100 {
            let buy_price = 900_000 + (i % 50) * 1000;
            let sell_price = 1_100_000 + (i % 50) * 1000;
            book.apply_event(&add_order_event(i * 2 + 1, Side::Buy, 100, buy_price));
            book.apply_event(&add_order_event(i * 2 + 2, Side::Sell, 150, sell_price));
        }
        assert!(book.is_consistent());

        for i in 0..100_000 {
            let old_ref = (i % 200) + 1;
            let new_ref = 10_000 + i;
            let side = if old_ref % 2 == 1 { Side::Buy } else { Side::Sell };
            let drift = (i % 100) as u64 * 500;
            let new_price = if side == Side::Buy { 800_000 + drift } else { 1_200_000 + drift };
            book.apply_event(&replace_order_event(old_ref, new_ref, 100 + (i % 500) as u32, new_price));
            assert!(
                book.is_consistent(),
                "Crossed book after replace #{}: bid={:?} ask={:?}",
                i,
                book.best_bid(),
                book.best_ask()
            );
        }
    }

    #[test]
    fn test_bbo_change_detection_no_false_positive() {
        let mut book = SingleOrderBook::new(TEST_STOCK);

        book.apply_event(&add_order_event(1, Side::Buy, 100, 1000000));
        book.apply_event(&add_order_event(2, Side::Sell, 100, 1100000));

        let bbo = book.apply_event(&add_order_event(3, Side::Buy, 50, 900000));
        assert!(bbo.is_none(), "Non-top add should not trigger BBO update");

        let bbo = book.apply_event(&add_order_event(4, Side::Sell, 50, 1200000));
        assert!(bbo.is_none(), "Non-top add should not trigger BBO update");
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
        assert!(ob.all_consistent());
    }

    #[test]
    fn test_multistock_replace() {
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
            order_ref: 100,
            side: Side::Buy,
            shares: 200,
            stock: STOCK_B,
            price: 2000000,
        });

        ob.apply_event(&ItchEvent::OrderReplace {
            timestamp: 2,
            order_ref: 1,
            new_order_ref: 2,
            shares: 300,
            price: 1500000,
        });

        let book_a = ob.get(STOCK_A).unwrap();
        assert_eq!(book_a.best_bid().unwrap().price, 1500000);
        assert_eq!(book_a.best_bid().unwrap().volume, 300);

        let book_b = ob.get(STOCK_B).unwrap();
        assert_eq!(book_b.best_bid().unwrap().price, 2000000);
        assert!(ob.all_consistent());
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

    #[test]
    fn test_concurrent_bbo_read_consistency() {
        let ob = Arc::new(parking_lot::Mutex::new(OrderBook::new()));
        const STOCK_A: u64 = 0x4141504C20202020;
        const STOCK_B: u64 = 0x4D53465420202020;

        {
            let mut guard = ob.lock();
            for i in 0..50 {
                guard.apply_event(&ItchEvent::AddOrder {
                    timestamp: i,
                    order_ref: i * 4 + 1,
                    side: Side::Buy,
                    shares: 100,
                    stock: STOCK_A,
                    price: 1_000_000 + i * 100,
                });
                guard.apply_event(&ItchEvent::AddOrder {
                    timestamp: i,
                    order_ref: i * 4 + 2,
                    side: Side::Sell,
                    shares: 100,
                    stock: STOCK_A,
                    price: 2_000_000 + i * 100,
                });
                guard.apply_event(&ItchEvent::AddOrder {
                    timestamp: i,
                    order_ref: i * 4 + 3,
                    side: Side::Buy,
                    shares: 100,
                    stock: STOCK_B,
                    price: 3_000_000 + i * 100,
                });
                guard.apply_event(&ItchEvent::AddOrder {
                    timestamp: i,
                    order_ref: i * 4 + 4,
                    side: Side::Sell,
                    shares: 100,
                    stock: STOCK_B,
                    price: 4_000_000 + i * 100,
                });
            }
        }

        let ob_writer = ob.clone();
        let writer = thread::spawn(move || {
            for i in 0..20_000 {
                let mut guard = ob_writer.lock();
                let order_ref = (i % 200) * 4 + 1;
                let new_ref = 100_000 + i;
                guard.apply_event(&ItchEvent::OrderReplace {
                    timestamp: i,
                    order_ref,
                    new_order_ref: new_ref,
                    shares: 100 + (i % 500) as u32,
                    price: 1_000_000 + (i % 200) as u64 * 100,
                });

                let order_ref = (i % 200) * 4 + 3;
                let new_ref = 500_000 + i;
                guard.apply_event(&ItchEvent::OrderReplace {
                    timestamp: i,
                    order_ref,
                    new_order_ref: new_ref,
                    shares: 100 + (i % 500) as u32,
                    price: 3_000_000 + (i % 200) as u64 * 100,
                });
                assert!(
                    guard.all_consistent(),
                    "Crossed book at writer iteration #{}",
                    i
                );
            }
        });

        let ob_reader = ob.clone();
        let reader = thread::spawn(move || {
            for _ in 0..20_000 {
                let guard = ob_reader.lock();
                if let Some(book) = guard.get(STOCK_A) {
                    let bid = book.best_bid().map(|l| l.price).unwrap_or(0);
                    let ask = book.best_ask().map(|l| l.price).unwrap_or(0);
                    if bid != 0 && ask != 0 {
                        assert!(
                            bid < ask,
                            "Crossed book read: bid={} ask={}",
                            bid,
                            ask
                        );
                    }
                }
                if let Some(book) = guard.get(STOCK_B) {
                    let bid = book.best_bid().map(|l| l.price).unwrap_or(0);
                    let ask = book.best_ask().map(|l| l.price).unwrap_or(0);
                    if bid != 0 && ask != 0 {
                        assert!(
                            bid < ask,
                            "Crossed book read: bid={} ask={}",
                            bid,
                            ask
                        );
                    }
                }
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();

        let guard = ob.lock();
        assert!(guard.all_consistent());
    }
}
