use std::{path::PathBuf, sync::Arc, time::Duration};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};

use countersyncd::{
    actor::local_storage::{benchmark_reduce, benchmark_write, LocalStorageConfig},
    message::saistats::{SAIStat, SAIStats},
};

const SERIES_COUNT: usize = 500;
const SAMPLE_COUNT: usize = 10_000;
const SAMPLE_INTERVAL_NS: u64 = 10_000;

fn samples() -> Arc<Vec<SAIStats>> {
    Arc::new(
        (0..SAMPLE_COUNT)
            .map(|sample| {
                SAIStats::new(
                    (sample as u64 + 1) * SAMPLE_INTERVAL_NS,
                    (0..SERIES_COUNT)
                        .map(|series| SAIStat {
                            object_name: format!("Ethernet{series}"),
                            type_id: 1,
                            stat_id: 2,
                            counter: sample as u64 * (series as u64 % 64 + 1),
                        })
                        .collect(),
                )
            })
            .collect(),
    )
}

fn config(root: PathBuf) -> LocalStorageConfig {
    LocalStorageConfig {
        root,
        range_interval: Duration::from_millis(10),
        shard_interval: Duration::from_secs(5),
        max_bytes: 4_000_000_000,
        require_dedicated_filesystem: false,
    }
}

fn benchmark_local_storage(c: &mut Criterion) {
    let samples = samples();
    let expected_rows = 5_500;
    assert_eq!(
        benchmark_reduce(samples.as_slice(), Duration::from_millis(10)),
        expected_rows
    );
    let mut group = c.benchmark_group("local_storage_10us_500_series");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements((SERIES_COUNT * SAMPLE_COUNT) as u64));

    group.bench_function("range_reducer", |bencher| {
        let samples = samples.clone();
        bencher.iter(|| benchmark_reduce(samples.as_slice(), Duration::from_millis(10)));
    });

    group.bench_function("range_reducer_and_single_gauge_parquet", |bencher| {
        let samples = samples.clone();
        bencher.iter_batched(
            || tempfile::tempdir().unwrap(),
            |root| {
                assert_eq!(
                    benchmark_write(samples.as_slice(), config(root.path().to_path_buf())),
                    expected_rows
                )
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(benches, benchmark_local_storage);
criterion_main!(benches);
