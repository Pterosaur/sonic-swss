use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io,
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt},
        io::AsRawFd,
    },
    path::{Path, PathBuf},
    sync::{mpsc, Arc},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::message::{
    aggregator::Heatmap,
    local_storage::{LocalStorageMessage, LocalStorageStatus},
};
use arrow_array::{
    types::{Float64Type, UInt64Type},
    ArrayRef, Float64Array, ListArray, RecordBatch, StringArray, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use log::{error, info};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
    format::KeyValue,
    schema::types::ColumnPath,
};

const FORMAT_VERSION: &str = "sonic-hft-parquet-v1";
const GAUGE_FILE: &str = "gauge_ranges.parquet";
const HEATMAP_FILE: &str = "heatmaps.parquet";
const READY_FILE: &str = "_READY";
const LOSS_FILE: &str = "loss.json";
const LOCK_FILE: &str = ".writer.lock";
const MAX_ROW_GROUP_ROWS: usize = 100_000;
const DATA_PAGE_SIZE: usize = 256 * 1024;
const WRITER_QUEUE_CAPACITY: usize = 1;
const SHARD_ROTATE_UNCOMPRESSED_BYTES: u64 = 120_000_000;
const MAX_SHARD_UNCOMPRESSED_BYTES: u64 = 128_000_000;
const SHARD_RESERVE_BYTES: u64 = 512_000_000;
const FILESYSTEM_RESERVE_BYTES: u64 = 512_000_000;

pub const RANGE_FLAG_DECREASED: u32 = 1;
pub const RANGE_FLAG_GAP: u32 = 1 << 1;
pub const RANGE_FLAG_STORAGE_DROP: u32 = 1 << 2;

#[derive(Debug, Clone)]
pub struct LocalStorageConfig {
    pub root: PathBuf,
    pub range_interval: std::time::Duration,
    pub shard_interval: std::time::Duration,
    pub max_bytes: u64,
    pub require_dedicated_filesystem: bool,
}

impl LocalStorageConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.range_interval.is_zero() {
            return Err("local storage range interval must be greater than zero".to_string());
        }
        if self.shard_interval.is_zero() {
            return Err("local storage shard interval must be greater than zero".to_string());
        }
        if self.max_bytes == 0 {
            return Err("local storage max bytes must be greater than zero".to_string());
        }
        if self.max_bytes <= SHARD_RESERVE_BYTES {
            return Err(format!(
                "local storage max bytes must exceed the {} byte shard reserve",
                SHARD_RESERVE_BYTES
            ));
        }
        if self.shard_interval < self.range_interval {
            return Err(
                "local storage shard interval must not be shorter than range interval".to_string(),
            );
        }
        Ok(())
    }

    pub fn validate_root(&self) -> Result<(), String> {
        self.validate()?;
        let metadata = fs::symlink_metadata(&self.root).map_err(|error| error.to_string())?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(format!(
                "local storage root {} must be an existing directory, not a symbolic link",
                self.root.display()
            ));
        }
        if self.require_dedicated_filesystem && !is_mount_point(&self.root)? {
            return Err(format!(
                "local storage root {} must be a dedicated mount point",
                self.root.display()
            ));
        }
        Ok(())
    }

    fn range_interval_ns(&self) -> u64 {
        self.range_interval.as_nanos().min(u128::from(u64::MAX)) as u64
    }

    fn shard_interval_ns(&self) -> u64 {
        self.shard_interval.as_nanos().min(u128::from(u64::MAX)) as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StreamKey {
    session: Option<Arc<str>>,
    source_template_id: Option<u16>,
}

#[derive(Debug)]
struct SeriesMetadata {
    object_name: Arc<str>,
    type_id: u32,
    stat_id: u32,
}

#[derive(Debug)]
struct StreamState {
    metadata: Vec<SeriesMetadata>,
    ranges: Vec<RangeState>,
    expected_interval_ns: Option<u64>,
    last_observation_time: u64,
}

#[derive(Debug)]
struct RangeState {
    window: u64,
    first_time_unix_nano: u64,
    last_time_unix_nano: u64,
    first_value: u64,
    previous_value: Option<u64>,
    last_value: u64,
    min_value: u64,
    max_value: u64,
    min_time_unix_nano: u64,
    max_time_unix_nano: u64,
    max_change: u64,
    max_change_time_unix_nano: u64,
    total_increase: u64,
    sample_count: u32,
    change_count: u32,
    flags: u32,
}

impl RangeState {
    fn new(window: u64, time_unix_nano: u64, value: u64, previous_value: Option<u64>) -> Self {
        let mut state = Self {
            window,
            first_time_unix_nano: time_unix_nano,
            last_time_unix_nano: time_unix_nano,
            first_value: value,
            previous_value,
            last_value: value,
            min_value: value,
            max_value: value,
            min_time_unix_nano: time_unix_nano,
            max_time_unix_nano: time_unix_nano,
            max_change: 0,
            max_change_time_unix_nano: time_unix_nano,
            total_increase: 0,
            sample_count: 1,
            change_count: 0,
            flags: 0,
        };
        if let Some(previous_value) = previous_value {
            state.record_change(time_unix_nano, previous_value, value);
        }
        state
    }

    fn update(&mut self, time_unix_nano: u64, value: u64) {
        let previous_value = self.last_value;
        self.record_change(time_unix_nano, previous_value, value);
        if value < self.min_value {
            self.min_value = value;
            self.min_time_unix_nano = time_unix_nano;
        }
        if value > self.max_value {
            self.max_value = value;
            self.max_time_unix_nano = time_unix_nano;
        }
        self.last_time_unix_nano = time_unix_nano;
        self.last_value = value;
        self.sample_count = self.sample_count.saturating_add(1);
    }

    fn record_change(&mut self, time_unix_nano: u64, previous_value: u64, value: u64) {
        if value != previous_value {
            self.change_count = self.change_count.saturating_add(1);
        }
        let change = value.abs_diff(previous_value);
        if change > self.max_change {
            self.max_change = change;
            self.max_change_time_unix_nano = time_unix_nano;
        }
        if value < previous_value {
            self.flags |= RANGE_FLAG_DECREASED;
        } else {
            self.total_increase = self.total_increase.saturating_add(change);
        }
    }
}

#[derive(Debug, Clone)]
struct GaugeRangeRow {
    session: Option<Arc<str>>,
    source_template_id: Option<u16>,
    object_name: Arc<str>,
    type_id: u32,
    stat_id: u32,
    window_start_unix_nano: u64,
    window_end_unix_nano: u64,
    first_time_unix_nano: u64,
    last_time_unix_nano: u64,
    first_value: u64,
    previous_value: Option<u64>,
    last_value: u64,
    min_value: u64,
    max_value: u64,
    min_time_unix_nano: u64,
    max_time_unix_nano: u64,
    max_change: u64,
    max_change_time_unix_nano: u64,
    total_increase: u64,
    sample_count: u32,
    change_count: u32,
    flags: u32,
}

impl GaugeRangeRow {
    fn from_state(
        key: &StreamKey,
        metadata: &SeriesMetadata,
        state: RangeState,
        interval_ns: u64,
    ) -> Self {
        let window_start_unix_nano = state.window.saturating_mul(interval_ns);
        Self {
            session: key.session.clone(),
            source_template_id: key.source_template_id,
            object_name: metadata.object_name.clone(),
            type_id: metadata.type_id,
            stat_id: metadata.stat_id,
            window_start_unix_nano,
            window_end_unix_nano: window_start_unix_nano.saturating_add(interval_ns),
            first_time_unix_nano: state.first_time_unix_nano,
            last_time_unix_nano: state.last_time_unix_nano,
            first_value: state.first_value,
            previous_value: state.previous_value,
            last_value: state.last_value,
            min_value: state.min_value,
            max_value: state.max_value,
            min_time_unix_nano: state.min_time_unix_nano,
            max_time_unix_nano: state.max_time_unix_nano,
            max_change: state.max_change,
            max_change_time_unix_nano: state.max_change_time_unix_nano,
            total_increase: state.total_increase,
            sample_count: state.sample_count,
            change_count: state.change_count,
            flags: state.flags,
        }
    }
}

#[derive(Debug, Clone)]
struct HeatmapRow {
    session: Option<Arc<str>>,
    heatmap: Heatmap,
}

#[derive(Default)]
struct ShardRows {
    gauges: Vec<GaugeRangeRow>,
    heatmaps: Vec<HeatmapRow>,
    dropped_input_messages: u64,
    dropped_shards: u64,
    estimated_uncompressed_bytes: u64,
}

impl ShardRows {
    fn push_gauge(&mut self, row: GaugeRangeRow) {
        self.estimated_uncompressed_bytes = self
            .estimated_uncompressed_bytes
            .saturating_add(176)
            .saturating_add(row.object_name.len() as u64)
            .saturating_add(row.session.as_ref().map_or(0, |value| value.len()) as u64);
        self.gauges.push(row);
    }

    fn push_heatmap(&mut self, row: HeatmapRow) {
        self.estimated_uncompressed_bytes = self
            .estimated_uncompressed_bytes
            .saturating_add(256)
            .saturating_add(row.heatmap.object_name.len() as u64)
            .saturating_add(row.heatmap.schema.len() as u64)
            .saturating_add(row.session.as_ref().map_or(0, |value| value.len()) as u64)
            .saturating_add((row.heatmap.explicit_bounds.len() as u64).saturating_mul(8))
            .saturating_add((row.heatmap.bucket_counts.len() as u64).saturating_mul(8));
        self.heatmaps.push(row);
    }
}

struct LocalReducer {
    range_interval_ns: u64,
    shard_interval_ns: u64,
    streams: HashMap<StreamKey, StreamState>,
    expected_intervals: HashMap<Arc<str>, u64>,
    shard_start: Option<u64>,
    rows: ShardRows,
    storage_drop_pending: bool,
}

impl LocalReducer {
    fn new(config: &LocalStorageConfig) -> Self {
        Self {
            range_interval_ns: config.range_interval_ns(),
            shard_interval_ns: config.shard_interval_ns(),
            streams: HashMap::new(),
            expected_intervals: HashMap::new(),
            shard_start: None,
            rows: ShardRows::default(),
            storage_drop_pending: false,
        }
    }

    fn add_gauges(
        &mut self,
        session: Option<Arc<str>>,
        source_template_id: Option<u16>,
        stats: &crate::message::saistats::SAIStats,
    ) {
        let time = stats.observation_time;
        let window = time / self.range_interval_ns;
        self.shard_start.get_or_insert(time);
        let key = StreamKey {
            session,
            source_template_id,
        };
        let expected_interval_ns = key
            .session
            .as_ref()
            .and_then(|session| self.expected_intervals.get(session.as_ref()))
            .copied();
        let Some(stream) = self.streams.get_mut(&key) else {
            let storage_drop_pending = self.storage_drop_pending;
            self.streams.insert(
                key,
                StreamState {
                    metadata: stats
                        .stats
                        .iter()
                        .map(|stat| SeriesMetadata {
                            object_name: Arc::from(stat.object_name.as_str()),
                            type_id: stat.type_id,
                            stat_id: stat.stat_id,
                        })
                        .collect(),
                    ranges: stats
                        .stats
                        .iter()
                        .map(|stat| {
                            let mut state = RangeState::new(window, time, stat.counter, None);
                            if storage_drop_pending {
                                state.flags |= RANGE_FLAG_STORAGE_DROP;
                            }
                            state
                        })
                        .collect(),
                    expected_interval_ns,
                    last_observation_time: time,
                },
            );
            return;
        };

        if time <= stream.last_observation_time {
            return;
        }
        let layout_changed = stream.metadata.len() != stats.stats.len()
            || (key.source_template_id.is_none()
                && stream
                    .metadata
                    .iter()
                    .zip(&stats.stats)
                    .any(|(metadata, stat)| {
                        metadata.object_name.as_ref() != stat.object_name
                            || metadata.type_id != stat.type_id
                            || metadata.stat_id != stat.stat_id
                    }));
        if layout_changed {
            Self::flush_stream(&mut self.rows, &key, stream, self.range_interval_ns);
            let storage_drop_pending = self.storage_drop_pending;
            *stream = StreamState {
                metadata: stats
                    .stats
                    .iter()
                    .map(|stat| SeriesMetadata {
                        object_name: Arc::from(stat.object_name.as_str()),
                        type_id: stat.type_id,
                        stat_id: stat.stat_id,
                    })
                    .collect(),
                ranges: stats
                    .stats
                    .iter()
                    .map(|stat| {
                        let mut state = RangeState::new(window, time, stat.counter, None);
                        if storage_drop_pending {
                            state.flags |= RANGE_FLAG_STORAGE_DROP;
                        }
                        state
                    })
                    .collect(),
                expected_interval_ns,
                last_observation_time: time,
            };
            return;
        }
        let previous_time = stream.last_observation_time;
        let observed_interval = time.saturating_sub(previous_time);
        if stream.expected_interval_ns.is_none() {
            stream.expected_interval_ns = Some(observed_interval);
        } else if let Some(expected) = stream.expected_interval_ns.as_mut() {
            *expected = (*expected).min(observed_interval);
        }
        if stream
            .ranges
            .first()
            .is_some_and(|state| window > state.window)
        {
            let skipped_window = stream
                .ranges
                .first()
                .is_some_and(|state| window > state.window.saturating_add(1));
            let previous_values = stream
                .ranges
                .iter()
                .map(|state| state.last_value)
                .collect::<Vec<_>>();
            Self::flush_stream(&mut self.rows, &key, stream, self.range_interval_ns);
            let gap = skipped_window
                || stream.expected_interval_ns.is_some_and(|interval| {
                    time.saturating_sub(previous_time) > interval.saturating_add(interval / 2)
                });
            stream.ranges = stats
                .stats
                .iter()
                .zip(previous_values)
                .map(|(stat, previous)| {
                    let mut state = RangeState::new(window, time, stat.counter, Some(previous));
                    if gap {
                        state.flags |= RANGE_FLAG_GAP;
                    }
                    if self.storage_drop_pending {
                        state.flags |= RANGE_FLAG_STORAGE_DROP;
                    }
                    state
                })
                .collect();
        } else if stream
            .ranges
            .first()
            .is_some_and(|state| window < state.window)
        {
            return;
        } else {
            let gap = stream.expected_interval_ns.is_some_and(|interval| {
                time.saturating_sub(previous_time) > interval.saturating_add(interval / 2)
            });
            for (state, stat) in stream.ranges.iter_mut().zip(&stats.stats) {
                state.update(time, stat.counter);
                if gap {
                    state.flags |= RANGE_FLAG_GAP;
                }
            }
        }
        stream.last_observation_time = time;
    }

    fn flush_stream(
        rows: &mut ShardRows,
        key: &StreamKey,
        stream: &mut StreamState,
        interval_ns: u64,
    ) {
        for (metadata, state) in stream.metadata.iter().zip(stream.ranges.drain(..)) {
            rows.push_gauge(GaugeRangeRow::from_state(key, metadata, state, interval_ns));
        }
    }

    fn add_heatmaps(&mut self, session: Option<Arc<str>>, heatmaps: &[Heatmap]) {
        if let Some(first) = heatmaps.first() {
            self.shard_start.get_or_insert(first.start_time_unix_nano);
        }
        for heatmap in heatmaps.iter().cloned() {
            self.rows.push_heatmap(HeatmapRow {
                session: session.clone(),
                heatmap,
            });
        }
    }

    fn reset_session(&mut self, key: Arc<str>, expected_interval_us: Option<u32>) {
        self.flush_session(&key);
        match expected_interval_us {
            Some(interval) => {
                self.expected_intervals
                    .insert(key, u64::from(interval).saturating_mul(1_000));
            }
            None => {
                self.expected_intervals.remove(&key);
            }
        }
    }

    fn remove_session(&mut self, key: &Arc<str>) {
        self.flush_session(key);
        self.expected_intervals.remove(key);
    }

    fn flush_session(&mut self, session: &Arc<str>) {
        let keys = self
            .streams
            .keys()
            .filter(|key| key.session.as_ref() == Some(session))
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(mut stream) = self.streams.remove(&key) {
                Self::flush_stream(&mut self.rows, &key, &mut stream, self.range_interval_ns);
            }
        }
    }

    fn should_rotate(&self, time_unix_nano: u64) -> bool {
        self.rows.estimated_uncompressed_bytes >= SHARD_ROTATE_UNCOMPRESSED_BYTES
            || self
                .shard_start
                .is_some_and(|start| time_unix_nano.saturating_sub(start) >= self.shard_interval_ns)
    }

    fn take_shard(&mut self, next_start_unix_nano: Option<u64>) -> ShardRows {
        self.shard_start = next_start_unix_nano;
        self.storage_drop_pending = false;
        std::mem::take(&mut self.rows)
    }

    fn flush_open_ranges(&mut self) {
        for (key, mut stream) in self.streams.drain() {
            Self::flush_stream(&mut self.rows, &key, &mut stream, self.range_interval_ns);
        }
    }

    fn finish(&mut self) -> ShardRows {
        self.flush_open_ranges();
        let mut rows = self.take_shard(None);
        if rows.gauges.is_empty()
            && rows.heatmaps.is_empty()
            && rows.dropped_input_messages == 0
            && rows.dropped_shards == 0
        {
            rows = ShardRows::default();
        }
        rows
    }

    fn mark_input_drop(&mut self, count: u64) {
        self.rows.dropped_input_messages = self.rows.dropped_input_messages.saturating_add(count);
        self.storage_drop_pending = true;
        self.mark_ranges_dropped();
    }

    fn mark_shard_drop(&mut self, count: u64) {
        self.rows.dropped_shards = self.rows.dropped_shards.saturating_add(count);
        self.storage_drop_pending = true;
        self.mark_ranges_dropped();
    }

    fn mark_ranges_dropped(&mut self) {
        for stream in self.streams.values_mut() {
            for range in &mut stream.ranges {
                range.flags |= RANGE_FLAG_STORAGE_DROP;
            }
        }
    }

    fn has_pending_rows(&self) -> bool {
        !self.rows.gauges.is_empty()
            || !self.rows.heatmaps.is_empty()
            || self.rows.dropped_input_messages != 0
            || self.rows.dropped_shards != 0
            || !self.streams.is_empty()
    }
}

pub struct LocalStorageActor {
    receiver: mpsc::Receiver<LocalStorageMessage>,
    config: LocalStorageConfig,
    status: LocalStorageStatus,
    storage_lock: File,
}

impl LocalStorageActor {
    pub fn new(
        receiver: mpsc::Receiver<LocalStorageMessage>,
        config: LocalStorageConfig,
        status: LocalStorageStatus,
    ) -> Result<Self, String> {
        let storage_lock = prepare_storage(&config)?;
        Ok(Self {
            receiver,
            config,
            status,
            storage_lock,
        })
    }

    pub fn run(self) {
        let mut reducer = LocalReducer::new(&self.config);
        let writer_config = self.config.clone();
        let writer_status = self.status.clone();
        let storage_lock = self.storage_lock;
        let (writer_sender, writer_receiver) = mpsc::sync_channel(WRITER_QUEUE_CAPACITY);
        let writer = thread::Builder::new()
            .name("hft-parquet-writer".to_string())
            .spawn(move || {
                writer_loop(writer_config, writer_receiver, writer_status, storage_lock)
            });
        let writer = match writer {
            Ok(writer) => writer,
            Err(reason) => {
                error!("Failed to start local HFT writer: {}", reason);
                return;
            }
        };
        let mut disabled = false;
        let mut last_rotation = Instant::now();

        loop {
            if self.status.failed() {
                break;
            }
            let message = match self.receiver.recv_timeout(Duration::from_millis(250)) {
                Ok(message) => message,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    let dropped = self.status.take_input_drops();
                    if dropped != 0 {
                        reducer.mark_input_drop(dropped);
                    }
                    if self.status.shutdown_requested() {
                        break;
                    }
                    if !disabled
                        && reducer.has_pending_rows()
                        && last_rotation.elapsed() >= self.config.shard_interval
                    {
                        reducer.flush_open_ranges();
                        let rows = reducer.take_shard(None);
                        if queue_shard(&writer_sender, rows, &mut reducer).is_err() {
                            disabled = true;
                        }
                        last_rotation = Instant::now();
                    }
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };
            let dropped = self.status.take_input_drops();
            if dropped != 0 {
                reducer.mark_input_drop(dropped);
            }
            if disabled {
                continue;
            }
            let time = match &message {
                LocalStorageMessage::Gauge { stats, .. } => stats.observation_time,
                LocalStorageMessage::Heatmaps { heatmaps, .. } => heatmaps
                    .last()
                    .map(|heatmap| heatmap.time_unix_nano)
                    .unwrap_or(0),
                LocalStorageMessage::ResetSession { .. }
                | LocalStorageMessage::RemoveSession { .. } => 0,
            };
            match message {
                LocalStorageMessage::Gauge {
                    key,
                    source_template_id,
                    stats,
                } => reducer.add_gauges(key, source_template_id, stats.as_ref()),
                LocalStorageMessage::Heatmaps { key, heatmaps } => {
                    reducer.add_heatmaps(key, heatmaps.as_ref())
                }
                LocalStorageMessage::ResetSession {
                    key,
                    expected_interval_us,
                } => reducer.reset_session(key, expected_interval_us),
                LocalStorageMessage::RemoveSession { key } => reducer.remove_session(&key),
            }
            if time != 0 && reducer.should_rotate(time) && reducer.has_pending_rows() {
                let rows = reducer.take_shard(Some(time));
                if queue_shard(&writer_sender, rows, &mut reducer).is_err() {
                    disabled = true;
                }
                last_rotation = Instant::now();
            }
        }

        if !disabled && !self.status.failed() {
            let rows = reducer.finish();
            if writer_sender.send(rows).is_err() {
                error!("Unable to queue final local HFT shard because the writer stopped");
            }
        }
        drop(writer_sender);
        if writer.join().is_err() {
            error!("Local HFT writer thread panicked");
        }
    }
}

fn queue_shard(
    sender: &mpsc::SyncSender<ShardRows>,
    rows: ShardRows,
    reducer: &mut LocalReducer,
) -> Result<(), ()> {
    match sender.try_send(rows) {
        Ok(()) => Ok(()),
        Err(mpsc::TrySendError::Full(rows)) => {
            error!("Dropping local HFT shard because the writer queue is full");
            reducer.mark_input_drop(rows.dropped_input_messages);
            reducer.mark_shard_drop(rows.dropped_shards.saturating_add(1));
            Ok(())
        }
        Err(mpsc::TrySendError::Disconnected(_)) => {
            error!("Disabling local HFT storage because the writer stopped");
            Err(())
        }
    }
}

fn is_mount_point(path: &Path) -> Result<bool, String> {
    let metadata = fs::metadata(path).map_err(|error| error.to_string())?;
    let parent = path
        .parent()
        .ok_or_else(|| format!("local storage root {} has no parent", path.display()))?;
    let parent_metadata = fs::metadata(parent).map_err(|error| error.to_string())?;
    Ok(metadata.dev() != parent_metadata.dev() || metadata.ino() == parent_metadata.ino())
}

fn writer_loop(
    config: LocalStorageConfig,
    receiver: mpsc::Receiver<ShardRows>,
    status: LocalStorageStatus,
    _storage_lock: File,
) {
    let mut committed_bytes = match directory_bytes(&config.root) {
        Ok(bytes) => bytes,
        Err(reason) => {
            error!("Local HFT writer could not measure storage: {}", reason);
            status.mark_failed();
            return;
        }
    };
    for (sequence, rows) in (0u64..).zip(receiver) {
        if let Err(reason) = write_shard(&config, sequence, rows, &mut committed_bytes) {
            error!("Local HFT writer stopped after write failure: {}", reason);
            status.mark_failed();
            return;
        }
    }
}

fn ensure_directory(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(format!("{} must not be a symbolic link", path.display()))
        }
        Ok(metadata) if !metadata.is_dir() => {
            Err(format!("{} must be a directory", path.display()))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(|error| error.to_string())
        }
        Err(error) => Err(error.to_string()),
    }
}

fn prepare_storage(config: &LocalStorageConfig) -> Result<File, String> {
    config.validate_root()?;
    let staging_root = config.root.join(".staging");
    let shards_root = config.root.join("shards");
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(config.root.join(LOCK_FILE))
        .map_err(|error| error.to_string())?;
    let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(format!(
            "local storage root is already in use: {}",
            io::Error::last_os_error()
        ));
    }
    ensure_directory(&staging_root)?;
    ensure_directory(&shards_root)?;
    sync_directory(&config.root).map_err(|error| error.to_string())?;
    for entry in fs::read_dir(&staging_root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            fs::remove_dir_all(path).map_err(|error| error.to_string())?;
        } else {
            fs::remove_file(path).map_err(|error| error.to_string())?;
        }
    }
    sync_directory(&staging_root).map_err(|error| error.to_string())?;
    Ok(lock)
}

fn gauge_schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("hft_session", DataType::Utf8, true),
            Field::new("source_template_id", DataType::UInt16, true),
            Field::new("object_name", DataType::Utf8, false),
            Field::new("sai_type_id", DataType::UInt32, false),
            Field::new("sai_stat_id", DataType::UInt32, false),
            Field::new("window_start_unix_nano", DataType::UInt64, false),
            Field::new("window_end_unix_nano", DataType::UInt64, false),
            Field::new("first_time_unix_nano", DataType::UInt64, false),
            Field::new("last_time_unix_nano", DataType::UInt64, false),
            Field::new("first_value", DataType::UInt64, false),
            Field::new("previous_value", DataType::UInt64, true),
            Field::new("last_value", DataType::UInt64, false),
            Field::new("min_value", DataType::UInt64, false),
            Field::new("max_value", DataType::UInt64, false),
            Field::new("min_time_unix_nano", DataType::UInt64, false),
            Field::new("max_time_unix_nano", DataType::UInt64, false),
            Field::new("max_change", DataType::UInt64, false),
            Field::new("max_change_time_unix_nano", DataType::UInt64, false),
            Field::new("total_increase", DataType::UInt64, false),
            Field::new("sample_count", DataType::UInt32, false),
            Field::new("change_count", DataType::UInt32, false),
            Field::new("flags", DataType::UInt32, false),
        ],
        HashMap::from([
            ("format_version".to_string(), FORMAT_VERSION.to_string()),
            (
                "window_semantics".to_string(),
                "[window_start_unix_nano,window_end_unix_nano)".to_string(),
            ),
            (
                "flags".to_string(),
                "1=decreased,2=source_gap,4=storage_drop".to_string(),
            ),
        ]),
    ))
}

fn heatmap_schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("hft_session", DataType::Utf8, true),
            Field::new("object_name", DataType::Utf8, false),
            Field::new("sai_type_id", DataType::UInt32, false),
            Field::new("sai_stat_id", DataType::UInt32, false),
            Field::new("start_time_unix_nano", DataType::UInt64, false),
            Field::new("time_unix_nano", DataType::UInt64, false),
            Field::new("count", DataType::UInt64, false),
            Field::new("sum", DataType::Float64, false),
            Field::new("min", DataType::UInt64, false),
            Field::new("max", DataType::UInt64, false),
            Field::new(
                "explicit_bounds",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                false,
            ),
            Field::new(
                "bucket_counts",
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
                false,
            ),
            Field::new("value_kind", DataType::Utf8, false),
            Field::new("quantity", DataType::Utf8, false),
            Field::new("unit", DataType::Utf8, false),
            Field::new("heatmap_schema", DataType::Utf8, false),
        ],
        HashMap::from([("format_version".to_string(), FORMAT_VERSION.to_string())]),
    ))
}

fn gauge_batch(rows: &[GaugeRangeRow]) -> Result<RecordBatch, String> {
    let schema = gauge_schema();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.session.as_deref())
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt16Array::from(
            rows.iter()
                .map(|row| row.source_template_id)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.object_name.as_ref()),
        )),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.type_id),
        )),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.stat_id),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.window_start_unix_nano),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.window_end_unix_nano),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.first_time_unix_nano),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.last_time_unix_nano),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.first_value),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.previous_value)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.last_value),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.min_value),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.max_value),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.min_time_unix_nano),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.max_time_unix_nano),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.max_change),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.max_change_time_unix_nano),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.total_increase),
        )),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.sample_count),
        )),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.change_count),
        )),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.flags),
        )),
    ];
    RecordBatch::try_new(schema, arrays).map_err(|error| error.to_string())
}

fn heatmap_batch(rows: &[HeatmapRow]) -> Result<RecordBatch, String> {
    let schema = heatmap_schema();
    let bounds = ListArray::from_iter_primitive::<Float64Type, _, _>(
        rows.iter()
            .map(|row| Some(row.heatmap.explicit_bounds.iter().copied().map(Some))),
    );
    let bucket_counts = ListArray::from_iter_primitive::<UInt64Type, _, _>(
        rows.iter()
            .map(|row| Some(row.heatmap.bucket_counts.iter().copied().map(Some))),
    );
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.session.as_deref())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.heatmap.object_name.as_ref()),
        )),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.heatmap.type_id),
        )),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.heatmap.stat_id),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.heatmap.start_time_unix_nano),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.heatmap.time_unix_nano),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.heatmap.count),
        )),
        Arc::new(Float64Array::from_iter_values(
            rows.iter().map(|row| row.heatmap.sum),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.heatmap.min),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.heatmap.max),
        )),
        Arc::new(bounds),
        Arc::new(bucket_counts),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.heatmap.value_kind.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.heatmap.quantity.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.heatmap.unit),
        )),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.heatmap.schema.as_ref()),
        )),
    ];
    RecordBatch::try_new(schema, arrays).map_err(|error| error.to_string())
}

fn writer_properties() -> Result<WriterProperties, String> {
    let zstd = ZstdLevel::try_new(1).map_err(|error| error.to_string())?;
    Ok(WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_key_value_metadata(Some(vec![KeyValue::new(
            "sonic_hft_format".to_string(),
            Some(FORMAT_VERSION.to_string()),
        )]))
        .set_compression(Compression::ZSTD(zstd))
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_data_page_size_limit(DATA_PAGE_SIZE)
        .set_max_row_group_size(MAX_ROW_GROUP_ROWS)
        .set_column_dictionary_enabled(ColumnPath::from("hft_session"), true)
        .set_column_dictionary_enabled(ColumnPath::from("object_name"), true)
        .set_column_dictionary_enabled(ColumnPath::from("value_kind"), true)
        .set_column_dictionary_enabled(ColumnPath::from("quantity"), true)
        .set_column_dictionary_enabled(ColumnPath::from("unit"), true)
        .set_column_dictionary_enabled(ColumnPath::from("heatmap_schema"), true)
        .set_column_encoding(
            ColumnPath::from("window_start_unix_nano"),
            Encoding::DELTA_BINARY_PACKED,
        )
        .set_column_encoding(
            ColumnPath::from("window_end_unix_nano"),
            Encoding::DELTA_BINARY_PACKED,
        )
        .build())
}

fn write_parquet(path: &Path, batch: RecordBatch) -> Result<(), String> {
    let file = File::create(path).map_err(|error| error.to_string())?;
    let sync_file = file.try_clone().map_err(|error| error.to_string())?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(writer_properties()?))
        .map_err(|error| error.to_string())?;
    writer.write(&batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    sync_file.sync_all().map_err(|error| error.to_string())
}

fn directory_bytes(path: &Path) -> io::Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        total = total.saturating_add(if metadata.is_dir() && !metadata.file_type().is_symlink() {
            directory_bytes(&entry.path())?
        } else {
            metadata.blocks().saturating_mul(512)
        });
    }
    Ok(total)
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn available_bytes(path: &Path) -> Result<u64, String> {
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| format!("path contains a NUL byte: {}", path.display()))?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let result = unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error().to_string());
    }
    let stats = unsafe { stats.assume_init() };
    Ok((stats.f_bavail as u64).saturating_mul(stats.f_frsize as u64))
}

fn write_shard(
    config: &LocalStorageConfig,
    sequence: u64,
    rows: ShardRows,
    committed_bytes: &mut u64,
) -> Result<(), String> {
    if rows.gauges.is_empty()
        && rows.heatmaps.is_empty()
        && rows.dropped_input_messages == 0
        && rows.dropped_shards == 0
    {
        return Ok(());
    }
    if rows.estimated_uncompressed_bytes > MAX_SHARD_UNCOMPRESSED_BYTES {
        return Err(format!(
            "local storage shard estimate {} exceeds {} byte limit",
            rows.estimated_uncompressed_bytes, MAX_SHARD_UNCOMPRESSED_BYTES
        ));
    }
    let staging_root = config.root.join(".staging");
    let shards_root = config.root.join("shards");

    if committed_bytes.saturating_add(SHARD_RESERVE_BYTES) > config.max_bytes {
        return Err(format!(
            "local storage quota cannot reserve next shard: {} committed + {} reserve > {} bytes",
            committed_bytes, SHARD_RESERVE_BYTES, config.max_bytes
        ));
    }
    let available = available_bytes(&config.root)?;
    let required = SHARD_RESERVE_BYTES.saturating_add(FILESYSTEM_RESERVE_BYTES);
    if available < required {
        return Err(format!(
            "local storage filesystem has {} bytes available; {} required",
            available, required
        ));
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?
        .as_nanos();
    let name = format!("{now:020}-{:010}-{sequence:06}", std::process::id());
    let staging = staging_root.join(&name);
    let destination = shards_root.join(&name);
    fs::create_dir(&staging).map_err(|error| error.to_string())?;

    let result = (|| {
        if !rows.gauges.is_empty() {
            write_parquet(&staging.join(GAUGE_FILE), gauge_batch(&rows.gauges)?)?;
        }
        if !rows.heatmaps.is_empty() {
            write_parquet(&staging.join(HEATMAP_FILE), heatmap_batch(&rows.heatmaps)?)?;
        }
        if rows.dropped_input_messages != 0 || rows.dropped_shards != 0 {
            let loss = staging.join(LOSS_FILE);
            fs::write(
                &loss,
                format!(
                    "{{\"dropped_input_messages\":{},\"dropped_shards\":{}}}\n",
                    rows.dropped_input_messages, rows.dropped_shards
                ),
            )
            .map_err(|error| error.to_string())?;
            File::open(&loss)
                .and_then(|file| file.sync_all())
                .map_err(|error| error.to_string())?;
        }
        let ready = staging.join(READY_FILE);
        fs::write(&ready, FORMAT_VERSION).map_err(|error| error.to_string())?;
        File::open(&ready)
            .and_then(|file| file.sync_all())
            .map_err(|error| error.to_string())?;
        sync_directory(&staging).map_err(|error| error.to_string())?;

        let staged_bytes = directory_bytes(&staging).map_err(|error| error.to_string())?;
        let next_bytes = committed_bytes.saturating_add(staged_bytes);
        if next_bytes > config.max_bytes {
            return Err(format!(
                "local storage shard would exceed quota: {} > {} bytes",
                next_bytes, config.max_bytes
            ));
        }

        fs::rename(&staging, &destination).map_err(|error| error.to_string())?;
        sync_directory(&shards_root).map_err(|error| error.to_string())?;
        sync_directory(&staging_root).map_err(|error| error.to_string())?;
        *committed_bytes = next_bytes;
        info!("Published local HFT shard {}", destination.display());
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result
}

#[doc(hidden)]
#[allow(dead_code)]
pub fn benchmark_reduce(
    samples: &[crate::message::saistats::SAIStats],
    range_interval: Duration,
) -> usize {
    let config = LocalStorageConfig {
        root: PathBuf::new(),
        range_interval,
        shard_interval: Duration::from_secs(5),
        max_bytes: 4_000_000_000,
        require_dedicated_filesystem: false,
    };
    let mut reducer = LocalReducer::new(&config);
    for sample in samples {
        reducer.add_gauges(Some(Arc::from("benchmark|PORT")), Some(256), sample);
    }
    reducer.finish().gauges.len()
}

#[doc(hidden)]
#[allow(dead_code)]
pub fn benchmark_write(
    samples: &[crate::message::saistats::SAIStats],
    config: LocalStorageConfig,
) -> usize {
    let mut reducer = LocalReducer::new(&config);
    for sample in samples {
        reducer.add_gauges(Some(Arc::from("benchmark|PORT")), Some(256), sample);
    }
    let rows = reducer.finish();
    let count = rows.gauges.len();
    write_parquet(
        &config.root.join(GAUGE_FILE),
        gauge_batch(&rows.gauges).unwrap(),
    )
    .unwrap();
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{
        aggregator::{HeatmapQuantity, HeatmapValueKind},
        local_storage::LocalStorageMessage,
        saistats::{SAIStat, SAIStats},
    };
    use arrow_array::{Array, Float64Array, ListArray, UInt16Array, UInt64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn stat(name: &str, value: u64) -> SAIStat {
        SAIStat {
            object_name: name.to_string(),
            type_id: 1,
            stat_id: 2,
            counter: value,
        }
    }

    #[test]
    fn summarizes_monotonic_range_and_decrease() {
        let config = LocalStorageConfig {
            root: PathBuf::new(),
            range_interval: std::time::Duration::from_millis(10),
            shard_interval: std::time::Duration::from_secs(5),
            max_bytes: 1,
            require_dedicated_filesystem: false,
        };
        let mut reducer = LocalReducer::new(&config);
        for (time, value) in [(1, 10), (101, 15), (201, 12), (10_000_000, 20)] {
            let stats =
                crate::message::saistats::SAIStats::new(time, vec![stat("Ethernet0", value)]);
            reducer.add_gauges(Some(Arc::from("session|PORT")), Some(256), &stats);
        }
        assert_eq!(reducer.rows.gauges.len(), 1);
        let row = &reducer.rows.gauges[0];
        assert_eq!(row.first_value, 10);
        assert_eq!(row.previous_value, None);
        assert_eq!(row.last_value, 12);
        assert_eq!(row.min_value, 10);
        assert_eq!(row.max_value, 15);
        assert_eq!(row.max_change, 5);
        assert_eq!(row.total_increase, 5);
        assert_eq!(row.sample_count, 3);
        assert_eq!(row.change_count, 2);
        assert_eq!(row.flags, RANGE_FLAG_DECREASED);

        let open = reducer.streams.values().next().unwrap();
        assert_eq!(open.ranges[0].previous_value, Some(12));
        assert_eq!(open.ranges[0].total_increase, 8);
    }

    #[test]
    fn writes_readable_parquet_bundle() {
        let temp = tempfile::tempdir().unwrap();
        let row = GaugeRangeRow {
            session: Some(Arc::from("session|PORT")),
            source_template_id: Some(256),
            object_name: Arc::from("Ethernet0"),
            type_id: 1,
            stat_id: 2,
            window_start_unix_nano: 0,
            window_end_unix_nano: 10_000_000,
            first_time_unix_nano: 1,
            last_time_unix_nano: 2,
            first_value: 10,
            previous_value: None,
            last_value: 20,
            min_value: 10,
            max_value: 20,
            min_time_unix_nano: 1,
            max_time_unix_nano: 2,
            max_change: 10,
            max_change_time_unix_nano: 2,
            total_increase: 10,
            sample_count: 2,
            change_count: 1,
            flags: 0,
        };
        let batch = gauge_batch(&[row]).unwrap();
        let path = temp.path().join(GAUGE_FILE);
        write_parquet(&path, batch).unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let first_value = batch
            .column_by_name("first_value")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(first_value.value(0), 10);
        let previous_value = batch
            .column_by_name("previous_value")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(previous_value.is_null(0));
        let template_id = batch
            .column_by_name("source_template_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert_eq!(template_id.value(0), 256);
    }

    #[test]
    fn actor_writes_gauge_and_heatmap_bundle_on_close() {
        let temp = tempfile::tempdir().unwrap();
        let config = LocalStorageConfig {
            root: temp.path().to_path_buf(),
            range_interval: Duration::from_millis(10),
            shard_interval: Duration::from_secs(5),
            max_bytes: 1_000_000_000,
            require_dedicated_filesystem: false,
        };
        let (sender, receiver) = mpsc::sync_channel(8);
        let status = LocalStorageStatus::default();
        let actor = LocalStorageActor::new(receiver, config, status).unwrap();
        let handle = thread::spawn(move || actor.run());

        sender
            .send(LocalStorageMessage::Gauge {
                key: Some(Arc::from("session|PORT")),
                source_template_id: Some(256),
                stats: Arc::new(SAIStats::new(1, vec![stat("Ethernet0", 10)])),
            })
            .unwrap();
        sender
            .send(LocalStorageMessage::Gauge {
                key: Some(Arc::from("session|PORT")),
                source_template_id: Some(256),
                stats: Arc::new(SAIStats::new(10_000_000, vec![stat("Ethernet0", 20)])),
            })
            .unwrap();
        sender
            .send(LocalStorageMessage::Heatmaps {
                key: Some(Arc::from("session|PORT")),
                heatmaps: vec![Heatmap {
                    object_name: Arc::from("Ethernet0"),
                    type_id: 1,
                    stat_id: 2,
                    start_time_unix_nano: 0,
                    time_unix_nano: 1_000_000_000,
                    count: 2,
                    sum: 10.0,
                    min: 4,
                    max: 6,
                    explicit_bounds: Arc::from([5.0]),
                    bucket_counts: vec![1, 1],
                    value_kind: HeatmapValueKind::Delta,
                    quantity: HeatmapQuantity::DeltaBytes,
                    unit: "By",
                    schema: Arc::from("test-schema"),
                }]
                .into(),
            })
            .unwrap();
        drop(sender);
        handle.join().unwrap();

        let shards = fs::read_dir(temp.path().join("shards"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(shards.len(), 1);
        assert!(shards[0].join(READY_FILE).is_file());
        let gauge_rows = ParquetRecordBatchReaderBuilder::try_new(
            File::open(shards[0].join(GAUGE_FILE)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum::<usize>();
        let heatmap_rows = ParquetRecordBatchReaderBuilder::try_new(
            File::open(shards[0].join(HEATMAP_FILE)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum::<usize>();
        assert_eq!(gauge_rows, 2);
        assert_eq!(heatmap_rows, 1);

        let mut reader = ParquetRecordBatchReaderBuilder::try_new(
            File::open(shards[0].join(HEATMAP_FILE)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let bounds = batch
            .column_by_name("explicit_bounds")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let bounds = bounds
            .value(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .to_vec();
        let counts = batch
            .column_by_name("bucket_counts")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let counts = counts
            .value(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(bounds, vec![5.0]);
        assert_eq!(counts, vec![1, 1]);
    }

    #[test]
    fn validates_limits() {
        let config = LocalStorageConfig {
            root: PathBuf::new(),
            range_interval: std::time::Duration::ZERO,
            shard_interval: std::time::Duration::from_secs(5),
            max_bytes: SHARD_RESERVE_BYTES + 1,
            require_dedicated_filesystem: false,
        };
        assert!(config.validate().is_err());

        let config = LocalStorageConfig {
            root: PathBuf::new(),
            range_interval: Duration::from_secs(10),
            shard_interval: Duration::from_secs(5),
            max_bytes: SHARD_RESERVE_BYTES + 1,
            require_dedicated_filesystem: false,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_non_dedicated_storage_root() {
        let temp = tempfile::tempdir().unwrap();
        let config = LocalStorageConfig {
            root: temp.path().to_path_buf(),
            range_interval: std::time::Duration::from_millis(10),
            shard_interval: std::time::Duration::from_secs(5),
            max_bytes: SHARD_RESERVE_BYTES + 1,
            require_dedicated_filesystem: true,
        };
        assert!(config.validate_root().is_err());
    }

    #[test]
    fn quota_reserve_prevents_starting_a_new_shard() {
        let temp = tempfile::tempdir().unwrap();
        let config = LocalStorageConfig {
            root: temp.path().to_path_buf(),
            range_interval: Duration::from_millis(10),
            shard_interval: Duration::from_secs(5),
            max_bytes: SHARD_RESERVE_BYTES + 1,
            require_dedicated_filesystem: false,
        };
        let _lock = prepare_storage(&config).unwrap();
        let mut committed_bytes = 2;
        let rows = ShardRows {
            dropped_input_messages: 1,
            ..Default::default()
        };
        assert!(write_shard(&config, 0, rows, &mut committed_bytes).is_err());
        assert_eq!(fs::read_dir(temp.path().join("shards")).unwrap().count(), 0);
    }

    #[test]
    fn drop_tracker_marks_the_next_open_range() {
        let config = LocalStorageConfig {
            root: PathBuf::new(),
            range_interval: Duration::from_millis(10),
            shard_interval: Duration::from_secs(5),
            max_bytes: SHARD_RESERVE_BYTES + 1,
            require_dedicated_filesystem: false,
        };
        let mut reducer = LocalReducer::new(&config);
        reducer.mark_input_drop(3);
        reducer.add_gauges(
            Some(Arc::from("session|PORT")),
            Some(256),
            &SAIStats::new(1, vec![stat("Ethernet0", 10)]),
        );
        reducer.mark_input_drop(2);
        let rows = reducer.finish();

        assert_eq!(rows.dropped_input_messages, 5);
        assert_eq!(rows.gauges.len(), 1);
        assert_eq!(rows.gauges[0].flags, RANGE_FLAG_STORAGE_DROP);
    }

    #[test]
    fn rejects_second_writer_for_same_root() {
        let temp = tempfile::tempdir().unwrap();
        let config = LocalStorageConfig {
            root: temp.path().to_path_buf(),
            range_interval: Duration::from_millis(10),
            shard_interval: Duration::from_secs(5),
            max_bytes: SHARD_RESERVE_BYTES + 1,
            require_dedicated_filesystem: false,
        };
        let _first = prepare_storage(&config).unwrap();
        assert!(prepare_storage(&config).is_err());
    }

    #[test]
    fn writes_loss_sidecar() {
        let temp = tempfile::tempdir().unwrap();
        let config = LocalStorageConfig {
            root: temp.path().to_path_buf(),
            range_interval: Duration::from_millis(10),
            shard_interval: Duration::from_secs(5),
            max_bytes: 1_000_000_000,
            require_dedicated_filesystem: false,
        };
        let _lock = prepare_storage(&config).unwrap();
        let mut committed_bytes = 0;
        write_shard(
            &config,
            0,
            ShardRows {
                dropped_input_messages: 7,
                dropped_shards: 2,
                ..Default::default()
            },
            &mut committed_bytes,
        )
        .unwrap();

        let shard = fs::read_dir(temp.path().join("shards"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert_eq!(
            fs::read_to_string(shard.join(LOSS_FILE)).unwrap(),
            "{\"dropped_input_messages\":7,\"dropped_shards\":2}\n"
        );
    }
}
