use iai_callgrind::{black_box, library_benchmark, library_benchmark_group, main};

use decentdb::benchmark::{
    append_wal_page_frame, crc32c_parts, decode_row, default_page_size, encode_row,
    intersect_sorted_postings, trigram_tokens, BtreeFixture,
};
use decentdb::Value;

#[library_benchmark]
fn wal_append_page_frame_4k() {
    let page_size = default_page_size();
    let payload = vec![0xA5_u8; page_size as usize];
    let mut out = Vec::with_capacity(payload.len() + 13);
    let written =
        append_wal_page_frame(&mut out, 7, black_box(&payload), page_size).expect("append");
    black_box(written);
    black_box(out);
}

#[library_benchmark]
fn crc32c_64k() {
    let payload = vec![0x5A_u8; 64 * 1024];
    let crc = crc32c_parts(&[black_box(payload.as_slice())]);
    black_box(crc);
}

#[library_benchmark]
fn btree_seek_point_lookup_warm_100k() {
    let fixture =
        BtreeFixture::with_sequential_keys(default_page_size(), 1, 100_000, 64).expect("fixture");
    let row = fixture
        .point_lookup(black_box(50_000))
        .expect("lookup")
        .expect("row");
    black_box(row);
}

#[library_benchmark]
fn btree_insert_split_1k_small_page() {
    let mut fixture = BtreeFixture::with_sequential_keys(512, 1, 1_024, 32).expect("fixture");
    fixture
        .insert_generated(200_000, 32)
        .expect("insert generated");
    black_box(fixture.len());
}

#[library_benchmark]
fn record_row_encode_decode_mixed_fields() {
    let values = vec![
        Value::Int64(42),
        Value::Text("alpha".repeat(8)),
        Value::Bool(true),
        Value::Float64(123.5),
        Value::Blob(vec![0xCC; 48]),
        Value::TimestampMicros(1_735_000_000),
    ];
    let encoded = encode_row(black_box(&values)).expect("encode");
    let decoded = decode_row(black_box(&encoded)).expect("decode");
    black_box(decoded);
}

#[library_benchmark]
fn trigram_tokenization() {
    let text = "decentdb trigram tokenization benchmark payload for hot path diagnostics";
    let tokens = trigram_tokens(black_box(text));
    black_box(tokens);
}

#[library_benchmark]
fn trigram_postings_intersection() {
    let postings = vec![
        (0_u64..20_000).step_by(2).collect::<Vec<_>>(),
        (5_000_u64..25_000).step_by(3).collect::<Vec<_>>(),
        (7_500_u64..30_000).step_by(5).collect::<Vec<_>>(),
    ];
    let intersection = intersect_sorted_postings(black_box(&postings));
    black_box(intersection);
}

library_benchmark_group!(
    name = micro_hot_paths_callgrind;
    benchmarks =
        wal_append_page_frame_4k,
        crc32c_64k,
        btree_seek_point_lookup_warm_100k,
        btree_insert_split_1k_small_page,
        record_row_encode_decode_mixed_fields,
        trigram_tokenization,
        trigram_postings_intersection
);

main!(library_benchmark_groups = micro_hot_paths_callgrind);
