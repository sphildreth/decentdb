# EF Core Conformance Skip List

This list tracks known EF Core relational conformance gaps for DecentDB provider coverage.

- Correlated subquery execution (`EXISTS`/derived FROM-item shapes emitted by EF for some LINQ patterns)
  - Reason: DecentDB engine currently rejects the generated FROM-item shape (`Unsupported FROM item`).
  - Tracking issue: https://github.com/sphildreth/decentdb/issues/20
- GroupBy aggregate execution emitted by EF relational pipeline
  - Reason: current DecentDB engine aggregate execution path rejects this generated query shape (`Aggregate functions evaluated elsewhere`).
  - Tracking issue: https://github.com/sphildreth/decentdb/issues/20
