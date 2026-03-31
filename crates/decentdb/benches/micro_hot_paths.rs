use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use decentdb::benchmark::{
    append_wal_page_frame, copy_page_bytes, crc32c_parts, decode_row, decode_wal_frame_payload_len,
    default_page_size, encode_index_key, encode_row, encode_wal_frame_page,
    intersect_sorted_postings, trigram_tokens, BtreeFixture,
};
use decentdb::Value;

fn bench_wal_frame_encode_decode(c: &mut Criterion) {
    let page_size = default_page_size();
    let payload = vec![0xA5_u8; page_size as usize];
    let encoded = encode_wal_frame_page(7, &payload, page_size).expect("encode frame");

    let mut group = c.benchmark_group("wal_frame");
    group.bench_function("wal_frame_encode_page_4k", |b| {
        b.iter(|| {
            let out = encode_wal_frame_page(black_box(7), black_box(&payload), page_size)
                .expect("encode frame");
            black_box(out);
        });
    });

    group.bench_function("wal_frame_decode_page_4k", |b| {
        b.iter(|| {
            let payload_len =
                decode_wal_frame_payload_len(black_box(&encoded), page_size).expect("decode");
            black_box(payload_len);
        });
    });
    group.finish();
}

fn bench_wal_append_path(c: &mut Criterion) {
    let page_size = default_page_size();
    let payload = vec![0xA5_u8; page_size as usize];

    let mut group = c.benchmark_group("wal_append");
    group.bench_function("wal_append_page_frame_4k", |b| {
        b.iter(|| {
            let mut out = Vec::with_capacity(payload.len() + 13);
            let written =
                append_wal_page_frame(&mut out, 7, &payload, page_size).expect("append frame");
            black_box(written);
            black_box(out);
        });
    });
    group.finish();
}

fn bench_checksum_and_copy(c: &mut Criterion) {
    let page_payload = vec![0x77_u8; default_page_size() as usize];
    let bulk_payload = vec![0x5A_u8; 64 * 1024];

    let mut group = c.benchmark_group("page_kernels");
    group.bench_function("page_copy_4k", |b| {
        b.iter(|| {
            let copied = copy_page_bytes(black_box(&page_payload));
            black_box(copied);
        });
    });

    group.throughput(Throughput::Bytes(bulk_payload.len() as u64));
    group.bench_function("crc32c_64k", |b| {
        b.iter(|| {
            let crc = crc32c_parts(&[black_box(bulk_payload.as_slice())]);
            black_box(crc);
        });
    });
    group.finish();
}

fn bench_btree_seek(c: &mut Criterion) {
    let fixture = BtreeFixture::with_sequential_keys(default_page_size(), 1, 100_000, 64)
        .expect("build fixture");

    let mut group = c.benchmark_group("btree");
    group.bench_function("btree_seek_point_lookup_warm_100k", |b| {
        b.iter(|| {
            let row = fixture
                .point_lookup(black_box(50_000))
                .expect("lookup")
                .expect("existing key");
            black_box(row);
        });
    });
    group.finish();
}

fn bench_btree_insert_split(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree");
    group.bench_function("btree_insert_split_1k_small_page", |b| {
        b.iter_batched(
            || BtreeFixture::with_sequential_keys(512, 1, 1_024, 32).expect("seed split fixture"),
            |mut fixture| {
                fixture
                    .insert_generated(200_000, 32)
                    .expect("insert generated key");
                black_box(fixture.len());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_record_encode_decode(c: &mut Criterion) {
    let values = vec![
        Value::Int64(42),
        Value::Text("alpha".repeat(8)),
        Value::Bool(true),
        Value::Float64(123.5),
        Value::Blob(vec![0xCC; 48]),
        Value::TimestampMicros(1_735_000_000),
    ];
    let encoded = encode_row(&values).expect("seed row");

    let mut group = c.benchmark_group("record");
    group.bench_function("record_row_encode_mixed_fields", |b| {
        b.iter(|| {
            let out = encode_row(black_box(&values)).expect("encode row");
            black_box(out);
        });
    });

    group.bench_function("record_row_decode_mixed_fields", |b| {
        b.iter(|| {
            let decoded = decode_row(black_box(&encoded)).expect("decode row");
            black_box(decoded);
        });
    });

    let key_values = [
        Value::Int64(123),
        Value::Int64(-9_876),
        Value::Text("key-alpha".repeat(4)),
        Value::Blob(vec![0x7A; 64]),
        Value::Uuid([0x11; 16]),
    ];
    for (idx, value) in key_values.iter().enumerate() {
        group.bench_with_input(
            BenchmarkId::new("record_index_key_encode", idx),
            value,
            |b, input| {
                b.iter(|| {
                    let out = encode_index_key(black_box(input)).expect("encode index key");
                    black_box(out);
                });
            },
        );
    }

    group.finish();
}

fn bench_trigram_kernels(c: &mut Criterion) {
    let text = "decentdb trigram tokenization benchmark payload for hot path diagnostics";
    let postings = vec![
        (0_u64..20_000).step_by(2).collect::<Vec<_>>(),
        (5_000_u64..25_000).step_by(3).collect::<Vec<_>>(),
        (7_500_u64..30_000).step_by(5).collect::<Vec<_>>(),
    ];

    let mut group = c.benchmark_group("trigram");
    group.bench_function("trigram_tokenization", |b| {
        b.iter(|| {
            let tokens = trigram_tokens(black_box(text));
            black_box(tokens);
        });
    });

    group.bench_function("trigram_postings_intersection", |b| {
        b.iter(|| {
            let intersection = intersect_sorted_postings(black_box(&postings));
            black_box(intersection);
        });
    });
    group.finish();
}

criterion_group!(
    micro_hot_paths,
    bench_wal_frame_encode_decode,
    bench_wal_append_path,
    bench_checksum_and_copy,
    bench_btree_seek,
    bench_btree_insert_split,
    bench_record_encode_decode,
    bench_trigram_kernels
);
criterion_main!(micro_hot_paths);
