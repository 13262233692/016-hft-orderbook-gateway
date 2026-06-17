use crate::orderbook::{PriceLevel, BboSnapshot};
use crate::pricing::{BsmResult, OptionType, solve_iv_and_greeks};
use ahash::AHashMap;
use arrayvec::ArrayVec;
use std::time::{SystemTime, UNIX_EPOCH};

pub const MSG_TYPE_SPOT_BBO: u8 = 0;
pub const MSG_TYPE_OPTION_SNAPSHOT: u8 = 1;

pub const MAX_CONTRACTS_PER_CHAIN: usize = 128;
pub const MAX_STRIKES_PER_EXPIRY: usize = 32;

pub const OPTION_CONTRACT_SERIALIZED_SIZE: usize = 121;
pub const OPTION_HEADER_SERIALIZED_SIZE: usize = 1 + 2 + 8 + 8 + 8 + 8 + 8 + 8 + 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OptionContractKey {
    pub underlying: u64,
    pub expiry_date: u32,
    pub strike_price: u64,
    pub option_type: OptionType,
}

fn date_to_unix_secs(date: u32) -> u64 {
    let year = date / 10000;
    let month = (date % 10000) / 100;
    let day = date % 100;

    let y = year as i64;
    let m = month as i64;
    let d = day as i64;

    let a = (14 - m) / 12;
    let y_adj = y + 4800 - a;
    let m_adj = m + 12 * a - 3;

    let jdn = d + (153 * m_adj + 2) / 5 + 365 * y_adj + y_adj / 4 - y_adj / 100 + y_adj / 400 - 32045;

    let unix_jdn: i64 = 2440588;
    ((jdn - unix_jdn) * 86400) as u64
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OptionContract {
    pub key: OptionContractKey,
    pub underlying: u64,
    pub expiry_date: u32,
    pub strike_price: u64,
    pub option_type: OptionType,
    pub lot_size: u32,
    pub creation_timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct OptionBook {
    contract: OptionContract,
    pub(crate) bids: AHashMap<u64, PriceLevel>,
    pub(crate) asks: AHashMap<u64, PriceLevel>,
    pub(crate) orders: AHashMap<u64, OptionOrderInfo>,
    pub(crate) last_bbo: Option<(u64, u64, u64, u64)>,
    pub(crate) last_pricing: Option<BsmResult>,
    pub(crate) last_spot_price: Option<f64>,
    pub(crate) pricing_dirty: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct OptionOrderInfo {
    pub side: crate::protocol::Side,
    pub price: u64,
    pub shares: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OptionQuote {
    pub contract_key: OptionContractKey,
    pub bid_price: u64,
    pub bid_volume: u64,
    pub ask_price: u64,
    pub ask_volume: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OptionGreeksSnapshot {
    pub contract_key: OptionContractKey,
    pub implied_volatility: f64,
    pub delta: f64,
    pub gamma: f64,
    pub vega: f64,
    pub theta: f64,
    pub rho: f64,
    pub model_price: f64,
    pub spot_price: f64,
    pub mid_price: f64,
}

#[derive(Debug, Clone)]
pub struct OptionChain {
    underlying: u64,
    contracts: AHashMap<OptionContractKey, OptionBook>,
    expiry_dates: Vec<u32>,
    risk_free_rate: f64,
    last_spot_price: Option<f64>,
    last_spot_timestamp: u64,
    chain_dirty: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregatedOptionSnapshot {
    pub underlying: u64,
    pub spot_bid_price: u64,
    pub spot_bid_volume: u64,
    pub spot_ask_price: u64,
    pub spot_ask_volume: u64,
    pub spot_timestamp: u64,
    pub contracts: ArrayVec<OptionContractSnapshot, MAX_CONTRACTS_PER_CHAIN>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OptionContractSnapshot {
    pub contract: OptionContract,
    pub bid_price: u64,
    pub bid_volume: u64,
    pub ask_price: u64,
    pub ask_volume: u64,
    pub implied_volatility: f64,
    pub delta: f64,
    pub gamma: f64,
    pub vega: f64,
    pub theta: f64,
    pub rho: f64,
    pub model_price: f64,
}

impl OptionContract {
    pub fn new(
        underlying: u64,
        expiry_date: u32,
        strike_price: u64,
        option_type: OptionType,
        lot_size: u32,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        Self {
            key: OptionContractKey {
                underlying,
                expiry_date,
                strike_price,
                option_type,
            },
            underlying,
            expiry_date,
            strike_price,
            option_type,
            lot_size,
            creation_timestamp: now,
        }
    }

    #[inline(always)]
    pub fn time_to_maturity(&self, current_timestamp_secs: u64) -> f64 {
        let expiry_ts = date_to_unix_secs(self.expiry_date);
        if expiry_ts <= current_timestamp_secs {
            return 0.0;
        }
        (expiry_ts - current_timestamp_secs) as f64 / (365.25 * 86400.0)
    }
}

impl OptionBook {
    pub fn new(contract: OptionContract) -> Self {
        Self {
            contract,
            bids: AHashMap::new(),
            asks: AHashMap::new(),
            orders: AHashMap::new(),
            last_bbo: None,
            last_pricing: None,
            last_spot_price: None,
            pricing_dirty: true,
        }
    }

    #[inline(always)]
    pub fn contract(&self) -> &OptionContract {
        &self.contract
    }

    #[inline(always)]
    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids.values().max_by_key(|l| l.price)
    }

    #[inline(always)]
    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks.values().min_by_key(|l| l.price)
    }

    #[inline(always)]
    pub fn bbo_snapshot(&self) -> (u64, u64, u64, u64) {
        let bid = self
            .best_bid()
            .map(|l| (l.price, l.volume))
            .unwrap_or((0, 0));
        let ask = self
            .best_ask()
            .map(|l| (l.price, l.volume))
            .unwrap_or((0, 0));
        (bid.0, bid.1, ask.0, ask.1)
    }

    #[inline(always)]
    pub fn mid_price(&self) -> Option<f64> {
        let (bid, _, ask, _) = self.bbo_snapshot();
        if bid > 0 && ask > 0 {
            Some((bid as f64 + ask as f64) / 2.0 / 10000.0)
        } else {
            None
        }
    }

    pub fn add_order(
        &mut self,
        order_ref: u64,
        side: crate::protocol::Side,
        price: u64,
        shares: u32,
    ) {
        self.orders.insert(
            order_ref,
            OptionOrderInfo {
                side,
                price,
                shares,
            },
        );

        let levels = match side {
            crate::protocol::Side::Buy => &mut self.bids,
            crate::protocol::Side::Sell => &mut self.asks,
        };

        match levels.get_mut(&price) {
            Some(level) => {
                level.volume += shares as u64;
                level.order_count += 1;
            }
            None => {
                levels.insert(
                    price,
                    PriceLevel::new(price, shares as u64, 1),
                );
            }
        }

        self.pricing_dirty = true;
    }

    pub fn remove_order(&mut self, order_ref: u64, shares: u32) {
        if let Some(info) = self.orders.get_mut(&order_ref) {
            let remove_shares = shares.min(info.shares);
            if remove_shares > 0 {
                info.shares -= remove_shares;

                let levels = match info.side {
                    crate::protocol::Side::Buy => &mut self.bids,
                    crate::protocol::Side::Sell => &mut self.asks,
                };

                if let Some(level) = levels.get_mut(&info.price) {
                    level.volume = level.volume.saturating_sub(remove_shares as u64);
                    level.order_count = level.order_count.saturating_sub(1);
                    if level.volume == 0 {
                        levels.remove(&info.price);
                    }
                }

                self.pricing_dirty = true;
            }

            if info.shares == 0 {
                self.orders.remove(&order_ref);
            }
        }
    }

    pub fn delete_order(&mut self, order_ref: u64) {
        if let Some(info) = self.orders.remove(&order_ref) {
            let levels = match info.side {
                crate::protocol::Side::Buy => &mut self.bids,
                crate::protocol::Side::Sell => &mut self.asks,
            };

            if let Some(level) = levels.get_mut(&info.price) {
                level.volume = level.volume.saturating_sub(info.shares as u64);
                level.order_count = level.order_count.saturating_sub(1);
                if level.volume == 0 {
                    levels.remove(&info.price);
                }
            }

            self.pricing_dirty = true;
        }
    }

    pub fn compute_pricing(
        &mut self,
        spot_price: f64,
        current_timestamp_secs: u64,
        risk_free_rate: f64,
    ) -> Option<OptionGreeksSnapshot> {
        let mid = self.mid_price()?;

        let ttm = self.contract.time_to_maturity(current_timestamp_secs);

        let strike_f = self.contract.strike_price as f64 / 10000.0;

        let result = solve_iv_and_greeks(
            mid,
            spot_price,
            strike_f,
            risk_free_rate,
            ttm,
            self.contract.option_type,
        );

        self.last_spot_price = Some(spot_price);
        self.last_pricing = result;
        self.pricing_dirty = false;
        self.last_bbo = Some(self.bbo_snapshot());

        result.map(|bsm| OptionGreeksSnapshot {
            contract_key: self.contract.key,
            implied_volatility: bsm.implied_volatility,
            delta: bsm.delta,
            gamma: bsm.gamma,
            vega: bsm.vega,
            theta: bsm.theta,
            rho: bsm.rho,
            model_price: bsm.price,
            spot_price,
            mid_price: mid,
        })
    }
}

impl OptionChain {
    pub fn new(underlying: u64, risk_free_rate: f64) -> Self {
        Self {
            underlying,
            contracts: AHashMap::new(),
            expiry_dates: Vec::new(),
            risk_free_rate,
            last_spot_price: None,
            last_spot_timestamp: 0,
            chain_dirty: false,
        }
    }

    #[inline(always)]
    pub fn underlying(&self) -> u64 {
        self.underlying
    }

    #[inline(always)]
    pub fn last_spot_price(&self) -> Option<f64> {
        self.last_spot_price
    }

    pub fn register_contract(&mut self, contract: OptionContract) {
        if !self.expiry_dates.contains(&contract.expiry_date) {
            self.expiry_dates.push(contract.expiry_date);
            self.expiry_dates.sort_unstable();
        }
        self.contracts
            .insert(contract.key, OptionBook::new(contract));
    }

    pub fn update_spot_price(&mut self, spot_price: f64, timestamp: u64) {
        self.last_spot_price = Some(spot_price);
        self.last_spot_timestamp = timestamp;
        self.chain_dirty = true;

        for book in self.contracts.values_mut() {
            book.pricing_dirty = true;
        }
    }

    pub fn get_contract(&self, key: &OptionContractKey) -> Option<&OptionBook> {
        self.contracts.get(key)
    }

    pub fn get_contract_mut(&mut self, key: &OptionContractKey) -> Option<&mut OptionBook> {
        self.contracts.get_mut(key)
    }

    pub fn update_all_pricing(&mut self, current_timestamp_secs: u64) -> Vec<OptionGreeksSnapshot> {
        let spot = match self.last_spot_price {
            Some(s) => s,
            None => return Vec::new(),
        };

        let mut results = Vec::new();
        for book in self.contracts.values_mut() {
            if book.pricing_dirty {
                if let Some(snapshot) =
                    book.compute_pricing(spot, current_timestamp_secs, self.risk_free_rate)
                {
                    results.push(snapshot);
                }
            }
        }

        self.chain_dirty = false;
        results
    }

    pub fn aggregate_snapshot(
        &mut self,
        spot_bbo: BboSnapshot,
        current_timestamp_secs: u64,
    ) -> Option<AggregatedOptionSnapshot> {
        let spot_price = (spot_bbo.bid_price as f64 + spot_bbo.ask_price as f64) / 2.0 / 10000.0;
        self.update_spot_price(spot_price, self.last_spot_timestamp);
        self.update_all_pricing(current_timestamp_secs);

        let mut contracts = ArrayVec::new();
        for book in self.contracts.values() {
            if let Some(pricing) = book.last_pricing {
                let (bid_price, bid_volume, ask_price, ask_volume) = book.bbo_snapshot();
                if bid_price > 0 || ask_price > 0 {
                    contracts.push(OptionContractSnapshot {
                        contract: book.contract,
                        bid_price,
                        bid_volume,
                        ask_price,
                        ask_volume,
                        implied_volatility: pricing.implied_volatility,
                        delta: pricing.delta,
                        gamma: pricing.gamma,
                        vega: pricing.vega,
                        theta: pricing.theta,
                        rho: pricing.rho,
                        model_price: pricing.price,
                    });
                }
            }
        }

        if contracts.is_empty() {
            return None;
        }

        Some(AggregatedOptionSnapshot {
            underlying: self.underlying,
            spot_bid_price: spot_bbo.bid_price,
            spot_bid_volume: spot_bbo.bid_volume,
            spot_ask_price: spot_bbo.ask_price,
            spot_ask_volume: spot_bbo.ask_volume,
            spot_timestamp: self.last_spot_timestamp,
            contracts,
        })
    }

    pub fn expirations(&self) -> &[u32] {
        &self.expiry_dates
    }

    pub fn strikes_for_expiry(&self, expiry: u32) -> Vec<u64> {
        let mut strikes: Vec<u64> = self
            .contracts
            .values()
            .filter(|c| c.contract.expiry_date == expiry)
            .map(|c| c.contract.strike_price)
            .collect();
        strikes.sort_unstable();
        strikes.dedup();
        strikes
    }

    pub fn len(&self) -> usize {
        self.contracts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.contracts.is_empty()
    }
}

#[derive(Debug, Default)]
pub struct OptionChainManager {
    chains: AHashMap<u64, OptionChain>,
    risk_free_rate: f64,
}

impl OptionChainManager {
    pub fn new(risk_free_rate: f64) -> Self {
        Self {
            chains: AHashMap::new(),
            risk_free_rate,
        }
    }

    pub fn get_or_create_chain(&mut self, underlying: u64) -> &mut OptionChain {
        self.chains
            .entry(underlying)
            .or_insert_with(|| OptionChain::new(underlying, self.risk_free_rate))
    }

    pub fn get_chain(&self, underlying: u64) -> Option<&OptionChain> {
        self.chains.get(&underlying)
    }

    pub fn get_chain_mut(&mut self, underlying: u64) -> Option<&mut OptionChain> {
        self.chains.get_mut(&underlying)
    }

    pub fn update_spot_price(&mut self, underlying: u64, spot_price: f64, timestamp: u64) {
        if let Some(chain) = self.chains.get_mut(&underlying) {
            chain.update_spot_price(spot_price, timestamp);
        }
    }

    pub fn register_contract(&mut self, contract: OptionContract) {
        let chain = self.get_or_create_chain(contract.underlying);
        chain.register_contract(contract);
    }

    pub fn apply_option_order_event(
        &mut self,
        event: &OptionOrderEvent,
    ) -> Option<OptionGreeksSnapshot> {
        let rate = self.risk_free_rate;
        let chain = self.get_or_create_chain(event.contract_key.underlying);

        {
            let book = chain.get_contract_mut(&event.contract_key)?;
            match event.action {
                OptionOrderAction::Add {
                    order_ref,
                    side,
                    price,
                    shares,
                } => {
                    book.add_order(order_ref, side, price, shares);
                }
                OptionOrderAction::Cancel { order_ref, shares } => {
                    book.remove_order(order_ref, shares);
                }
                OptionOrderAction::Delete { order_ref } => {
                    book.delete_order(order_ref);
                }
            }
        }

        let spot = chain.last_spot_price()?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let book = chain.get_contract_mut(&event.contract_key)?;
        book.compute_pricing(spot, now, rate)
    }

    pub fn chains(&self) -> impl Iterator<Item = (&u64, &OptionChain)> {
        self.chains.iter()
    }

    pub fn chains_mut(&mut self) -> impl Iterator<Item = (&u64, &mut OptionChain)> {
        self.chains.iter_mut()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OptionOrderAction {
    Add {
        order_ref: u64,
        side: crate::protocol::Side,
        price: u64,
        shares: u32,
    },
    Cancel {
        order_ref: u64,
        shares: u32,
    },
    Delete {
        order_ref: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OptionOrderEvent {
    pub contract_key: OptionContractKey,
    pub action: OptionOrderAction,
    pub timestamp: u64,
}

impl OptionOrderEvent {
    pub fn add(
        contract: OptionContractKey,
        order_ref: u64,
        side: crate::protocol::Side,
        price: u64,
        shares: u32,
        timestamp: u64,
    ) -> Self {
        Self {
            contract_key: contract,
            action: OptionOrderAction::Add {
                order_ref,
                side,
                price,
                shares,
            },
            timestamp,
        }
    }

    pub fn cancel(
        contract: OptionContractKey,
        order_ref: u64,
        shares: u32,
        timestamp: u64,
    ) -> Self {
        Self {
            contract_key: contract,
            action: OptionOrderAction::Cancel {
                order_ref,
                shares,
            },
            timestamp,
        }
    }

    pub fn delete(
        contract: OptionContractKey,
        order_ref: u64,
        timestamp: u64,
    ) -> Self {
        Self {
            contract_key: contract,
            action: OptionOrderAction::Delete {
                order_ref,
            },
            timestamp,
        }
    }
}

impl OptionContractSnapshot {
    #[inline(always)]
    pub fn serialize(&self, buf: &mut [u8; OPTION_CONTRACT_SERIALIZED_SIZE]) {
        let mut pos = 0;
        buf[pos..pos + 4].copy_from_slice(&self.contract.expiry_date.to_le_bytes());
        pos += 4;
        buf[pos..pos + 8].copy_from_slice(&self.contract.strike_price.to_le_bytes());
        pos += 8;
        buf[pos] = match self.contract.option_type {
            OptionType::Call => 0,
            OptionType::Put => 1,
        };
        pos += 1;
        buf[pos..pos + 8].copy_from_slice(&self.bid_price.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.bid_volume.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.ask_price.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.ask_volume.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.implied_volatility.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.delta.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.gamma.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.vega.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.theta.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.rho.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.model_price.to_le_bytes());
    }
}

impl AggregatedOptionSnapshot {
    pub fn serialized_size(&self) -> usize {
        OPTION_HEADER_SERIALIZED_SIZE + self.contracts.len() * OPTION_CONTRACT_SERIALIZED_SIZE
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        let total_len = self.serialized_size() as u16;
        buf.push(MSG_TYPE_OPTION_SNAPSHOT);
        buf.extend_from_slice(&total_len.to_le_bytes());
        buf.extend_from_slice(&self.underlying.to_le_bytes());
        buf.extend_from_slice(&self.spot_bid_price.to_le_bytes());
        buf.extend_from_slice(&self.spot_bid_volume.to_le_bytes());
        buf.extend_from_slice(&self.spot_ask_price.to_le_bytes());
        buf.extend_from_slice(&self.spot_ask_volume.to_le_bytes());
        buf.extend_from_slice(&self.spot_timestamp.to_le_bytes());
        buf.extend_from_slice(&(self.contracts.len() as u16).to_le_bytes());

        for contract in &self.contracts {
            let mut contract_buf = [0u8; OPTION_CONTRACT_SERIALIZED_SIZE];
            contract.serialize(&mut contract_buf);
            buf.extend_from_slice(&contract_buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pricing::OptionType;
    use crate::protocol::Side;

    const TEST_UNDERLYING: u64 = 0x4141504C20202020;

    #[test]
    fn test_option_contract_ttm() {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let contract = OptionContract::new(TEST_UNDERLYING, 20270101, 1_500_000, OptionType::Call, 100);

        let ttm = contract.time_to_maturity(now_secs);
        assert!(ttm > 0.4 && ttm < 0.8, "ttm = {}, expected ~0.5", ttm);
    }

    #[test]
    fn test_option_book_add_and_bbo() {
        let contract = OptionContract::new(TEST_UNDERLYING, 20261217, 1_500_000, OptionType::Call, 100);
        let mut book = OptionBook::new(contract);

        book.add_order(1, Side::Buy, 50_000, 10);
        book.add_order(2, Side::Buy, 45_000, 20);
        book.add_order(3, Side::Sell, 55_000, 15);
        book.add_order(4, Side::Sell, 60_000, 25);

        let (bid_p, bid_v, ask_p, ask_v) = book.bbo_snapshot();
        assert_eq!(bid_p, 50_000);
        assert_eq!(bid_v, 10);
        assert_eq!(ask_p, 55_000);
        assert_eq!(ask_v, 15);

        let mid = book.mid_price().unwrap();
        assert_eq!(mid, 5.25);
    }

    #[test]
    fn test_option_pricing_calculation() {
        let contract = OptionContract::new(TEST_UNDERLYING, 20261217, 1_000_000, OptionType::Call, 100);
        let mut book = OptionBook::new(contract);

        book.add_order(1, Side::Buy, 70_000, 10);
        book.add_order(2, Side::Sell, 72_000, 15);

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let greeks = book.compute_pricing(100.0, now_secs, 0.03);
        assert!(greeks.is_some());

        let greeks = greeks.unwrap();
        assert!(greeks.implied_volatility > 0.0);
        assert!(greeks.delta > 0.0 && greeks.delta < 1.0);
        assert!(greeks.gamma > 0.0);
    }

    #[test]
    fn test_option_chain_manager() {
        let mut manager = OptionChainManager::new(0.03);

        let contract1 = OptionContract::new(TEST_UNDERLYING, 20261217, 1_500_000, OptionType::Call, 100);
        let contract2 = OptionContract::new(TEST_UNDERLYING, 20261217, 1_500_000, OptionType::Put, 100);
        let contract3 = OptionContract::new(TEST_UNDERLYING, 20270317, 1_500_000, OptionType::Call, 100);

        manager.register_contract(contract1);
        manager.register_contract(contract2);
        manager.register_contract(contract3);

        let chain = manager.get_chain(TEST_UNDERLYING).unwrap();
        assert_eq!(chain.len(), 3);
        assert_eq!(chain.expirations().len(), 2);

        manager.update_spot_price(TEST_UNDERLYING, 150.0, 1234567890);

        let add_buy = OptionOrderEvent::add(
            contract1.key,
            1,
            Side::Buy,
            50_000,
            10,
            1234567891,
        );
        let _ = manager.apply_option_order_event(&add_buy);

        let add_sell = OptionOrderEvent::add(
            contract1.key,
            2,
            Side::Sell,
            55_000,
            15,
            1234567892,
        );

        let result = manager.apply_option_order_event(&add_sell);
        assert!(result.is_some());
    }
}
