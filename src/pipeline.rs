use crate::orderbook::BboUpdate;
use crate::options::{AggregatedOptionSnapshot, OptionOrderEvent};
use crate::protocol::ItchEvent;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

pub const EVENT_QUEUE_CAP: usize = 1 << 16;
pub const BBO_QUEUE_CAP: usize = 1 << 14;
pub const OPTION_EVENT_QUEUE_CAP: usize = 1 << 16;
pub const OPTION_SNAPSHOT_QUEUE_CAP: usize = 1 << 14;

pub struct Pipeline {
    pub event_tx: Arc<ArrayQueue<ItchEvent>>,
    pub bbo_rx: Arc<ArrayQueue<BboUpdate>>,
    pub option_event_tx: Arc<ArrayQueue<OptionOrderEvent>>,
    pub option_snapshot_rx: Arc<ArrayQueue<AggregatedOptionSnapshot>>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            event_tx: Arc::new(ArrayQueue::new(EVENT_QUEUE_CAP)),
            bbo_rx: Arc::new(ArrayQueue::new(BBO_QUEUE_CAP)),
            option_event_tx: Arc::new(ArrayQueue::new(OPTION_EVENT_QUEUE_CAP)),
            option_snapshot_rx: Arc::new(ArrayQueue::new(OPTION_SNAPSHOT_QUEUE_CAP)),
        }
    }

    #[inline(always)]
    pub fn push_event(&self, event: ItchEvent) -> Result<(), ItchEvent> {
        self.event_tx.push(event)
    }

    #[inline(always)]
    pub fn pop_bbo(&self) -> Option<BboUpdate> {
        self.bbo_rx.pop()
    }

    #[inline(always)]
    pub fn push_option_event(&self, event: OptionOrderEvent) -> Result<(), OptionOrderEvent> {
        self.option_event_tx.push(event)
    }

    #[inline(always)]
    pub fn pop_option_event(&self) -> Option<OptionOrderEvent> {
        self.option_event_tx.pop()
    }

    #[inline(always)]
    pub fn push_option_snapshot(
        &self,
        snapshot: AggregatedOptionSnapshot,
    ) -> Result<(), AggregatedOptionSnapshot> {
        self.option_snapshot_rx.push(snapshot)
    }

    #[inline(always)]
    pub fn pop_option_snapshot(&self) -> Option<AggregatedOptionSnapshot> {
        self.option_snapshot_rx.pop()
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}
