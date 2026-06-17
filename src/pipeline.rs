use crate::orderbook::BboUpdate;
use crate::protocol::ItchEvent;
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

pub const EVENT_QUEUE_CAP: usize = 1 << 16;
pub const BBO_QUEUE_CAP: usize = 1 << 14;

pub struct Pipeline {
    pub event_tx: Arc<ArrayQueue<ItchEvent>>,
    pub bbo_rx: Arc<ArrayQueue<BboUpdate>>,
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            event_tx: Arc::new(ArrayQueue::new(EVENT_QUEUE_CAP)),
            bbo_rx: Arc::new(ArrayQueue::new(BBO_QUEUE_CAP)),
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
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}
