# HFT local storage

`countersyncd --enable-local-storage` writes bounded, best-effort HFT summaries to
the dedicated `/mnt/hft` filesystem. The feature is disabled by default.

The fixed safety profile is:

| Setting | Value |
| --- | ---: |
| Gauge range interval | 10 ms |
| Immutable shard interval | 5 seconds |
| Application quota | 4,000,000,000 bytes |
| Per-shard rotation target | 120,000,000 estimated uncompressed bytes |
| Per-shard estimate limit | 128,000,000 uncompressed bytes |
| Filesystem free-space reserve | 512,000,000 bytes |
| Input queue capacity | 1,024 messages |

`/mnt/hft` must already be a separate mount point. Payloads must not be stored in
`/var/log` or on an unbounded system filesystem.

## Directory layout

```text
/mnt/hft/
  .writer.lock
  .staging/
  shards/
    <unix-ns>-<pid>-<sequence>/
      gauge_ranges.parquet
      heatmaps.parquet
      loss.json             # present only if input or shard data was dropped
      _READY
```

A shard is visible to readers only after every Parquet file is closed and synced,
`_READY` is synced, and the staging directory is atomically renamed. Startup
deletes incomplete staging directories. Only one writer may hold `.writer.lock`.

## Gauge ranges

Gauge input remains the raw IPFIX `UINT64` value. Values are summarized into epoch-aligned,
half-open `[window_start_unix_nano, window_end_unix_nano)` windows. A row stores:

- session, source template, object, SAI object type, and SAI statistic ID
- first, last, minimum, and maximum values and their observation times
- value immediately before the window, if available
- total monotonic increase, maximum absolute change and its observation time
- sample/change counts and quality flags

Flags are bitwise:

| Bit | Meaning |
| ---: | --- |
| 1 | Counter decreased in this range |
| 2 | Source sample gap or skipped range window |
| 4 | Local input message or complete shard was dropped |

The reducer is intentionally lossy: it preserves ranges and significant change
timing, not every sample. Parquet integer encoding and ZSTD level 1 are lossless
with respect to these stored rows. A decrease identifies a possible rollover,
reset, or invalid source transition; the local recorder does not infer which one.

## Heatmaps

Completed aggregator heatmaps are stored using OpenTelemetry explicit-histogram
fields: start/end time, count, sum, min, max, explicit bounds, bucket counts,
value kind, quantity, unit, and schema ID. The invariant is:

```text
bucket_counts.length = explicit_bounds.length + 1
sum(bucket_counts) = count
```

## Reading

DuckDB can query the files directly:

```sql
SELECT
    window_start_unix_nano,
    object_name,
    first_value,
    last_value,
    min_value,
    max_value,
    max_change,
    flags
FROM read_parquet('/mnt/hft/shards/*/gauge_ranges.parquet')
WHERE object_name = 'Ethernet0'
ORDER BY window_start_unix_nano;

SELECT
    start_time_unix_nano,
    object_name,
    explicit_bounds,
    bucket_counts
FROM read_parquet('/mnt/hft/shards/*/heatmaps.parquet')
ORDER BY start_time_unix_nano;
```

Only directories containing `_READY` are committed. Consumers should inspect
`loss.json` and gauge flags before treating a time range as complete.

## Reliability scope

Local storage is optional and cannot block the critical IPFIX pipeline. Gauge
ranges use raw IPFIX values; existing Aggregator rollover correction is not
applied to the local range table. A full
input queue drops messages and records the loss. A full writer queue drops a
complete shard and records that loss in a later shard. `ENOSPC`, `EIO`, `EROFS`,
quota exhaustion, or writer failure disables local output for the process.
The quota is append-until-stop, not circular retention; no old shard is deleted.

These guarantees do not imply that the complete Netlink/IPFIX pipeline sustains
500 counters at a 10 microsecond interval. That end-to-end target requires
separate Netlink and IPFIX hot-path work.

The `local_storage_perf` Criterion benchmark measures an isolated reducer and a
single gauge Parquet write using prebuilt `SAIStats`. Its throughput is reported
as input values per second. It does not include Netlink, IPFIX parsing, actor
queues, heatmap computation, shard publication, or sustained fsync latency.
