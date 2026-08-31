use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use super::aggregator::Heatmap;
use super::saistats::SAIStatsMessage;

#[derive(Debug, Clone)]
pub enum LocalStorageMessage {
    Gauge {
        key: Option<Arc<str>>,
        source_template_id: Option<u16>,
        stats: SAIStatsMessage,
    },
    Heatmaps {
        key: Option<Arc<str>>,
        heatmaps: Arc<[Heatmap]>,
    },
    ResetSession {
        key: Arc<str>,
        expected_interval_us: Option<u32>,
    },
    RemoveSession {
        key: Arc<str>,
    },
}

#[derive(Debug, Clone, Default)]
pub struct LocalStorageStatus {
    dropped: Arc<AtomicU64>,
    failed: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
}

impl LocalStorageStatus {
    pub fn record_input_drop(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn take_input_drops(&self) -> u64 {
        self.dropped.swap(0, Ordering::Relaxed)
    }

    pub fn mark_failed(&self) {
        self.failed.store(true, Ordering::Release);
    }

    pub fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    pub fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    pub fn shutdown_requested(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}
