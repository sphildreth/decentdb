# Runtime Advisors And Fix-Plan Policy
**Date:** 2026-06-09
**Status:** Accepted

### Decision

DecentDB will add runtime advisors as evidence-based analysis that consumes
runtime trace snapshots, shipped operational `sys.*` metrics, catalog metadata,
statistics, and Doctor facts. Advisors produce findings and recommendations;
they do not directly mutate schema, weaken durability, or apply destructive
changes.

Runtime advisor findings use the Doctor finding model where possible. Every
advisor finding must include:

- stable finding ID;
- advisor ID;
- severity;
- confidence;
- category;
- title;
- message;
- bounded redacted evidence;
- recommendation summary;
- optional recommended command;
- optional recommended SQL;
- `safe_to_automate`;
- trace-window metadata when derived from tracing.

Severity reuses Doctor's existing values:

- `info`
- `warning`
- `error`

Confidence is a separate field with these stable values:

- `low`
- `medium`
- `high`

Advisor categories begin with:

- `query`
- `indexes`
- `schema`
- `contention`
- `wal`
- `storage`
- `stats`
- `compatibility`
- `sync`
- `branch`
- `browser`
- `mobile`

The first runtime advisor families are:

- query-plan advisor;
- missing-index candidate advisor;
- conservative unused-index advisor;
- contention advisor;
- WAL/storage advisor.

Schema lint, redundant-index analysis, JSON path advice, sync diagnostics,
branch diagnostics, browser diagnostics, and mobile diagnostics may be added as
later phases under the same finding model.

The query-plan advisor must distinguish execution cost from lock wait. A
statement that is slow because it waited behind a writer must not produce a
missing-index recommendation unless there is separate evidence of expensive
scan, sort, filter, or row production work.

The missing-index advisor may suggest SQL text for reviewed migrations, but it
must not create indexes automatically. Missing-index suggestions require
evidence from slow or high-cost statements, parsed predicates or ordering,
table cardinality or statistics, absence of an equivalent usable index, and a
write-maintenance risk note. If object naming or quoting cannot be made safe,
the advisor must omit generated SQL and provide a textual recommendation only.

The unused-index advisor must be conservative. It may report that an index had
no observed read use during a stated observation window, but it must not claim
that the index is globally unused. It must consider constraint and uniqueness
roles, write maintenance count, observation window length, reset/eviction
state, table write volume, and unsupported index families. Dropping an index is
not safe to automate in the initial advisor implementation.

The contention advisor consumes lock-wait traces, write queue metrics, process
coordination metrics, WAL metrics, and session state. It may recommend
shortening transactions, closing idle readers, adjusting checkpoint cadence,
moving maintenance to quieter windows, or collecting a support bundle. It must
not change transaction semantics, lock behavior, process coordination mode, or
durability settings.

The WAL/storage advisor may improve existing Doctor recommendations by adding
runtime evidence such as reader retention, checkpoint blockers, write bursts,
and storage I/O delays. It must not change checkpoint semantics or WAL policy.

Advisor evidence must be redacted and bounded. It may include event IDs,
statement fingerprints, object names subject to redaction policy, plan
summaries, row-count estimates, observed counts, wait durations, and trace
window metadata. It must not include raw parameter values or raw row values by
default.

Doctor will gain a `--fix-plan` mode. A fix plan is a review artifact that
lists possible remediation actions, preconditions, risks, commands, suggested
SQL, and automation classification. It does not apply changes.

Fix-plan action classifications are:

- safe automated maintenance;
- safe but requires exclusive access;
- migration suggestion, manual review required;
- destructive, never automatic;
- unsupported in current runtime.

Existing `doctor --fix` behavior remains narrow and explicit. It may continue
to apply only documented safe fixes such as checkpointing or rebuilding stale
or invalid indexes when existing preconditions pass. The presence of a
fix-plan action must not imply that `doctor --fix` will execute it.

The initial advisor implementation must set `safe_to_automate = false` for:

- `CREATE INDEX` suggestions;
- `DROP INDEX` suggestions;
- durability setting changes;
- process coordination mode changes;
- schema rewrites;
- destructive maintenance;
- changes requiring application-specific transaction or workload knowledge.

### Rationale

Runtime tracing creates evidence, but evidence alone is not guidance. Advisors
turn evidence into reviewable recommendations while preserving DecentDB's
preference for boring, explicit, durable behavior.

The main risk is overconfident automation. Missing-index and unused-index
advice can be useful, but it is workload-dependent and can increase write
amplification or break application assumptions if applied blindly. Doctor
already has a recommendation model and a narrow safe-fix model, so runtime
advisors should extend that model instead of creating a separate automation
surface.

Separating severity from confidence makes findings clearer. A high-confidence
`info` finding can be useful but not urgent. A low-confidence `warning` can
tell users where to investigate without pretending certainty.

`doctor --fix-plan` gives automation a stable artifact without expanding the
set of actions that Doctor applies automatically.

### Alternatives Considered

1. **Automatically create missing indexes.** Rejected. Index choice depends on
   workload shape, write cost, naming, migration policy, storage budget, and
   rollback planning.
2. **Automatically drop unused indexes.** Rejected. A bounded observation
   window does not prove global non-use, and indexes may enforce constraints.
3. **Use natural-language recommendations only.** Rejected. Users need
   machine-readable findings, evidence, confidence, and fix-plan actions.
4. **Create a separate advisor output model unrelated to Doctor.** Rejected.
   That would duplicate severity, recommendation, JSON, and automation
   concepts already present in Doctor.
5. **Treat all performance findings as warnings.** Rejected. Many tuning hints
   are informational unless they cause clear contention, storage growth, or
   user-visible latency.
6. **Let `doctor --fix-plan` apply changes with a dry-run flag.** Rejected.
   Planning and applying are separate commands. `--fix-plan` is inspection.

### Trade-offs

- Conservative advisors may miss some valid optimization opportunities.
- Requiring evidence and confidence adds implementation complexity.
- Generated SQL suggestions need careful quoting and collision handling.
- Keeping schema changes manual limits "one-click tuning", but it avoids
  surprising users and protects durable production data.
- Reusing Doctor's finding model may require report schema extensions and
  compatibility tests.

### Consequences

- Doctor JSON must include advisor ID, confidence, trace-window metadata, and
  fix-plan references when advisor findings are present.
- Advisor tests must include false-positive cases, especially lock-wait slow
  queries that should not trigger missing-index advice.
- `doctor --fix-plan` JSON must be versioned and documented.
- Existing `doctor --fix` tests must prove the automatic fix set did not
  silently expand to schema mutations.
- Documentation must state that advisors are recommendations, not proof, and
  that generated SQL requires review.
- Any future automatic schema change advisor requires a follow-up ADR.

### References

- `design/WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md`
- `design/adr/0163-operational-sys-metrics.md`
- `design/adr/0185-rich-structured-error-diagnostics-contract.md`
- `docs/user-guide/doctor.md`
- `docs/api/sql-functions.md`

