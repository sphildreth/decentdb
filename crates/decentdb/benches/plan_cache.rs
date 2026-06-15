//! Plan cache benchmarks (ADR 0190-0193 validation slice).
//!
//! The benchmark set covers:
//! - repeated prepare throughput (cache enabled/disabled),
//! - one-shot overhead on the same dataset (cache enabled/disabled), and
//! - warm churn with p95/p99 latency reporting.
//!
//! Enabled vs disabled runs intentionally share the same SQL payloads and
//! dataset shape so the only variable is plan cache configuration.

use std::hint::black_box;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};
use decentdb::{Db, DbConfig, Value};
use tempfile::TempDir;

const PLAN_CACHE_BENCH_ROWS: i64 = 10_000;
const CHURN_VARIANTS: usize = 1_000;
const CHURN_WORKING_SET_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;

fn open_db_with_plan_cache(
    plan_cache_enabled: bool,
    max_cache_bytes: Option<u64>,
) -> (TempDir, Db) {
    let temp = TempDir::new().expect("tempdir");
    let db_path = temp.path().join("plan_cache_bench.ddb");
    let mut config = DbConfig::default();
    config.with_plan_cache(|cfg| {
        cfg.enabled = plan_cache_enabled;
        if let Some(max_cache_bytes) = max_cache_bytes {
            cfg.max_size_bytes = max_cache_bytes;
        }
    });
    let db = Db::create(&db_path, config).expect("create database");

    db.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, val TEXT, grp INTEGER)")
        .expect("create bench table");
    for i in 0..PLAN_CACHE_BENCH_ROWS {
        db.execute_with_params(
            "INSERT INTO bench VALUES ($1, $2, $3)",
            &[
                Value::Int64(i),
                Value::Text(format!("v{i}")),
                Value::Int64(i % 10),
            ],
        )
        .expect("seed bench rows");
    }
    (temp, db)
}

fn build_point_lookup_statements(row_count: i64, variants: usize) -> Vec<String> {
    let row_count = usize::try_from(row_count).expect("rows fit in usize");
    (0..variants)
        .map(|idx| {
            let id = idx % row_count;
            format!("SELECT COUNT(*) FROM bench WHERE id = {id}")
        })
        .collect()
}

fn percentile_ms_ns(samples: &mut [u64], percentile: u32) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    samples.sort_unstable();
    let idx = (samples.len().saturating_sub(1) * percentile as usize).div_ceil(100);
    samples[idx]
}

fn repeated_prepare_point_lookup(c: &mut Criterion, plan_cache_enabled: bool) {
    let (_temp, db) = open_db_with_plan_cache(plan_cache_enabled, None);
    let mode = if plan_cache_enabled {
        "enabled"
    } else {
        "disabled"
    };
    let name = format!("plan_cache/repeated_prepare_point_lookup/{mode}");

    c.bench_function(
        &name,
        |b| {
            b.iter_custom(|iters| {
                let iters = usize::try_from(iters).expect("sample size fits usize");
                db.flush_plan_cache().expect("flush plan cache");
                let before = db.plan_cache_summary().expect("read plan cache summary before");
                let start = Instant::now();
                for i in 0..iters {
                    let id = (i as i64) % PLAN_CACHE_BENCH_ROWS;
                    let prepared = db
                        .prepare("SELECT val FROM bench WHERE id = $1")
                        .expect("prepare point lookup");
                    prepared
                        .execute(&[Value::Int64(id)])
                        .expect("execute point lookup");
                }
                let elapsed = start.elapsed();
                let after = db.plan_cache_summary().expect("read plan cache summary after");
                let hit_delta = after.total_hits.saturating_sub(before.total_hits);
                let miss_delta = after.total_misses.saturating_sub(before.total_misses);
                if plan_cache_enabled {
                    let expected_warm_hits = iters.saturating_sub(1) as u64;
                    assert!(
                        hit_delta >= expected_warm_hits,
                        "expected cache hits for repeated prepare workload; got hits={hit_delta}, iters={iters}"
                    );
                } else {
                    assert_eq!(
                        hit_delta, 0,
                        "cache disabled should not report cache hits"
                    );
                }
                if plan_cache_enabled {
                    assert!(
                        miss_delta >= 1,
                        "enabled benchmark should record at least one miss on cold start"
                    );
                } else {
                    assert_eq!(miss_delta, 0, "cache disabled should not record misses");
                }
                black_box((hit_delta, miss_delta));
                elapsed
            })
        },
    );
}

fn one_shot_overhead(c: &mut Criterion, plan_cache_enabled: bool) {
    let (_temp, db) = open_db_with_plan_cache(plan_cache_enabled, None);
    let mode = if plan_cache_enabled {
        "enabled"
    } else {
        "disabled"
    };
    let name = format!("plan_cache/one_shot_query/{mode}");

    c.bench_function(&name, |b| {
        b.iter_custom(|iters| {
            let iters = usize::try_from(iters).expect("sample size fits usize");
            db.flush_plan_cache().expect("flush plan cache");
            let before = db
                .plan_cache_summary()
                .expect("read plan cache summary before");
            let statements = (0..iters)
                .map(|i| {
                    let id = (i as i64) % PLAN_CACHE_BENCH_ROWS;
                    format!("SELECT COUNT(*) FROM bench WHERE id = {id} AND {i} = {i}")
                })
                .collect::<Vec<_>>();
            let start = Instant::now();
            for stmt in &statements {
                db.execute(stmt).expect("execute one-shot query");
            }
            let elapsed = start.elapsed();
            let after = db
                .plan_cache_summary()
                .expect("read plan cache summary after");
            let hit_delta = after.total_hits.saturating_sub(before.total_hits);
            let miss_delta = after.total_misses.saturating_sub(before.total_misses);

            assert_eq!(
                hit_delta, 0,
                "one-shot query stream should not see cached hits"
            );
            if plan_cache_enabled {
                assert!(
                    miss_delta == 0 || miss_delta >= iters as u64,
                    "one-shot stream should either bypass zero-parameter cache lookup or miss for each unique statement"
                );
            } else {
                assert_eq!(miss_delta, 0, "cache disabled should not report misses");
            }
            black_box((hit_delta, miss_delta));
            elapsed
        })
    });
}

fn churn_p95_p99(c: &mut Criterion, plan_cache_enabled: bool) {
    let (_temp, db) = open_db_with_plan_cache(
        plan_cache_enabled,
        if plan_cache_enabled {
            Some(CHURN_WORKING_SET_CACHE_MAX_BYTES)
        } else {
            None
        },
    );
    let mode = if plan_cache_enabled {
        "enabled"
    } else {
        "disabled"
    };
    let name = format!("plan_cache/churn_p95_p99/{mode}");
    let statements = build_point_lookup_statements(PLAN_CACHE_BENCH_ROWS, CHURN_VARIANTS);
    let mut cursor = 0usize;
    let mut printed_latency_summary = false;

    db.flush_plan_cache().expect("flush plan cache");
    if plan_cache_enabled {
        for stmt in statements.iter() {
            let prepared = db.prepare(stmt).expect("prepare churn warm statement");
            black_box(prepared);
        }
        let warmed = db
            .plan_cache_summary()
            .expect("read warmed plan cache summary");
        assert!(
            warmed.total_entries >= CHURN_VARIANTS as u64,
            "warm churn workload should fit in the benchmark cache budget"
        );
    }

    c.bench_function(&name, |b| {
        b.iter_custom(|iters| {
            let iters = usize::try_from(iters).expect("sample size fits usize");
            let before = db.plan_cache_summary().expect("read plan cache summary before");

            let mut latencies_ns = Vec::with_capacity(iters);
            let start = Instant::now();
            for _ in 0..iters {
                let stmt = &statements[cursor % statements.len()];
                cursor = cursor.wrapping_add(1);
                let op_start = Instant::now();
                let prepared = db.prepare(stmt).expect("prepare churn statement");
                black_box(prepared);
                latencies_ns.push(
                    u64::try_from(op_start.elapsed().as_nanos()).unwrap_or(u64::MAX),
                );
            }
            let elapsed = start.elapsed();

            let p95_ns = percentile_ms_ns(&mut latencies_ns, 95);
            let p99_ns = percentile_ms_ns(&mut latencies_ns, 99);
            let after = db.plan_cache_summary().expect("read plan cache summary after");
            let hit_delta = after.total_hits.saturating_sub(before.total_hits);
            let miss_delta = after.total_misses.saturating_sub(before.total_misses);

            if plan_cache_enabled {
                assert!(
                    hit_delta >= iters as u64,
                    "warm churn workload should hit after the working set is prepared"
                );
            } else {
                assert_eq!(
                    hit_delta, 0,
                    "cache disabled should not report cache hits in churn workload"
                );
                assert_eq!(
                    miss_delta, 0,
                    "cache disabled should not report misses in churn workload"
                );
            }
            if !printed_latency_summary && iters >= 16 {
                println!(
                    "plan_cache/churn_p95_p99/{mode}: p95={p95_ns}ns p99={p99_ns}ns hits_delta={hit_delta} misses_delta={miss_delta}"
                );
                printed_latency_summary = true;
            }
            black_box((hit_delta, miss_delta, p95_ns, p99_ns));
            elapsed
        })
    });
}

fn bench_plan_cache(c: &mut Criterion) {
    repeated_prepare_point_lookup(c, true);
    repeated_prepare_point_lookup(c, false);

    one_shot_overhead(c, true);
    one_shot_overhead(c, false);

    churn_p95_p99(c, true);
    churn_p95_p99(c, false);
}

criterion_group!(
    name = plan_cache_benches;
    config = Criterion::default();
    targets = bench_plan_cache
);

criterion_main!(plan_cache_benches);
