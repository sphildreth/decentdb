# DecentDB Showcase Benchmark Prompt

Use this prompt with coding agents to generate a **language-specific, scalable, benchmarkable DecentDB sample project**.

---

## Wrapper Prompt Template

Replace the placeholders before giving this to a coding agent.

```text
Implement the following specification in {{TARGET_LANGUAGE}}.

Additional implementation requirements:
- Use idiomatic {{TARGET_LANGUAGE}} project structure.
- Keep the benchmark runner straightforward, readable, and easy to run locally.
- Support scale factor {{SCALE_FACTOR}}x base.
- Support benchmark profile "{{BENCHMARK_PROFILE}}".
- Export benchmark results to JSON.
- Keep the database and SQL as the primary focus; do not over-engineer the surrounding application.

Now follow this specification exactly:

[BEGIN SPECIFICATION]
{{CORE_SPECIFICATION}}
[END SPECIFICATION]
```

### Example wrapper values

- `{{TARGET_LANGUAGE}}` = `C#`
- `{{TARGET_LANGUAGE}}` = `Dart`
- `{{TARGET_LANGUAGE}}` = `Rust`
- `{{TARGET_LANGUAGE}}` = `Python`
- `{{TARGET_LANGUAGE}}` = `Go`
- `{{TARGET_LANGUAGE}}` = `TypeScript`

- `{{SCALE_FACTOR}}` = `1`
- `{{SCALE_FACTOR}}` = `10`
- `{{SCALE_FACTOR}}` = `25`

- `{{BENCHMARK_PROFILE}}` = `smoke`
- `{{BENCHMARK_PROFILE}}` = `balanced`
- `{{BENCHMARK_PROFILE}}` = `full`
- `{{BENCHMARK_PROFILE}}` = `stress`

---

## Core Specification

```text
# Task

Create a **DecentDB showcase project** for the domain:

**Incident Response / Operations Command Center**

Implement the surrounding application in:

`{{TARGET_LANGUAGE}}`

Use a data scale of:

`{{SCALE_FACTOR}}x base`

Optional benchmark profile:

`{{BENCHMARK_PROFILE}}`

Examples:
- `TARGET_LANGUAGE = C#`, `SCALE_FACTOR = 10`, `BENCHMARK_PROFILE = full`
- `TARGET_LANGUAGE = Dart`, `SCALE_FACTOR = 25`, `BENCHMARK_PROFILE = full`
- `TARGET_LANGUAGE = Rust`, `SCALE_FACTOR = 5`, `BENCHMARK_PROFILE = balanced`

---

## Primary Goal

Build a polished, realistic, non-e-commerce DecentDB sample that demonstrates:

1. relational schema design
2. meaningful SQL usage
3. realistic seed data generation
4. tunable scale-factor-based data generation
5. repeatable developer-focused benchmarks
6. clear reporting of results

The result should feel like an official sample and benchmark project for DecentDB.

---

## Domain Model

Model an incident operations system with entities such as:

- teams
- users/responders
- services
- service dependencies
- alert sources
- alerts
- incidents
- incident_services
- incident_responders
- incident_timeline
- deployments
- maintenance_windows
- runbooks
- audit_log

Do not replace this domain with e-commerce, products, carts, storefronts, or generic CRUD examples.

---

## Schema Requirements

Create a normalized relational schema with about 10–14 tables.

Include:
- primary keys
- foreign keys
- unique constraints
- check constraints
- default values
- generated columns where useful

Include index examples where supported and appropriate:
- standard B-tree indexes
- expression indexes
- partial indexes
- covering indexes
- trigram indexes

Create at least:
- 3 meaningful views
- 2 practical triggers

Do not invent unsupported DecentDB features.

---

## Data Generation Requirements

Implement a deterministic synthetic data generator.

Use a `SCALE_FACTOR` parameter so that the dataset size can be increased or decreased without changing schema logic.

The generator must:
- produce realistic relational data
- preserve foreign key integrity
- generate believable distributions
- avoid purely uniform random data
- support repeatable runs using a fixed seed

### Base Dataset

Use a base dataset approximately like this:

- teams: 10
- users/responders: 100
- services: 75
- service_dependencies: 150
- alert_sources: 8
- deployments: 500
- alerts: 5,000
- incidents: 1,000
- incident_services: 2,500
- incident_responders: 2,000
- incident_timeline: 10,000
- maintenance_windows: 200
- runbooks: 150

Then scale using `SCALE_FACTOR`.

### Scaling Rules

Do not simply multiply every table by the same number unless justified.

Prefer realistic scaling, for example:
- teams grow slowly
- users and services grow linearly
- alerts grow faster than services
- timeline events grow faster than incidents
- dependencies grow proportionally to service count
- audit log grows from workload execution

Document the scaling rules clearly.

---

## Benchmark Requirements

Create a benchmark harness that runs repeatable tests and reports clear metrics developers care about.

### Benchmark Categories

At minimum benchmark:

#### 1. Initialization
- schema creation time
- index creation time
- seed data generation time
- data load time
- final database size

#### 2. Insert Performance
- single-row inserts
- batched inserts
- inserts involving foreign keys
- inserts that fire triggers
- inserts affecting indexed/generated-column tables

#### 3. Read Performance
- primary key lookup
- indexed lookup
- range query
- multi-table join
- aggregate report
- CTE report
- supported window function query
- trigram/text search query
- JSON expansion query using `json_each(...)`
- query against a view
- temp table or temp view analysis flow

#### 4. Update Performance
- single-row update
- batched updates
- indexed-column updates
- trigger-firing updates
- workflow-style incident state change update

#### 5. Delete Performance
- single-row delete
- batched cleanup delete
- archive-style delete workflow if appropriate

#### 6. Mixed Workflow Scenarios
Benchmark realistic business workflows, such as:
- create incident with related alerts and responders
- append timeline events during an incident
- resolve incident and write audit entries
- run dashboard query pack
- search incidents and drill into one incident

---

## Benchmark Measurement Expectations

For each benchmark, capture:

- benchmark name
- query or operation type
- number of iterations
- total elapsed time
- average latency
- median latency if practical
- min/max latency if practical
- rows processed if applicable
- rows returned if applicable
- scale factor used
- final database size if relevant

If practical in `{{TARGET_LANGUAGE}}`, also capture:
- standard deviation
- p95 latency
- p99 latency

If those are too heavy for the chosen implementation, state that clearly.

---

## Benchmark Profiles

Support benchmark profiles such as:

### smoke
Minimal and quick:
- reduced iterations
- intended for correctness verification

### balanced
Reasonable benchmark duration:
- moderate iterations
- intended for local developer use

### full
More rigorous:
- more iterations
- suitable for comparing languages / implementations / scales

### stress
Heavy run:
- intended for high-scale testing on powerful machines

Document how each profile changes iteration counts.

---

## Query and Workload Requirements

Provide at least 15 showcase SQL queries.

These must demonstrate:
- joins
- aggregates
- CTEs
- supported window functions
- view queries
- temp table or temp view usage
- `json_each(...)`
- search patterns
- index-friendly lookups
- reporting queries

For each query, include:
- short description
- what DecentDB feature it demonstrates
- why a developer would care about it

---

## Language-Specific Application Requirements

Implement the surrounding code in `{{TARGET_LANGUAGE}}`.

The application should:
- initialize the database
- create schema
- create indexes
- load seed data
- run showcase queries
- run benchmark suite
- print readable results
- optionally export results as JSON or CSV

The code should be simple, idiomatic, and easy to run locally.

Do not let framework complexity overshadow the database and benchmark logic.

---

## Reporting Requirements

Produce a README that explains:
- project purpose
- why this is a good DecentDB showcase
- schema overview
- scale factor model
- benchmark methodology
- benchmark profiles
- how to run at different scales
- how to compare runs across languages
- known limitations / assumptions

Also produce a machine-readable benchmark output format such as:
- `benchmark_results.json`
- optionally `benchmark_results.csv`

---

## CLI / Configuration Requirements

Support configuration through command-line args or config file.

At minimum allow:
- target database path
- scale factor
- benchmark profile
- random seed
- whether to rebuild database
- whether to run only schema load
- whether to run only seed/load
- whether to run only showcase queries
- whether to run only benchmarks
- whether to export results

Example desired usage:

- `run --scale 1 --profile smoke`
- `run --scale 10 --profile balanced`
- `run --scale 25 --profile full --export-json`
- `run --scale 10 --profile full --seed 42`

---

## Suggested Project Layout

Structure the output so it is easy to compare across languages. Prefer something close to:

- `schema.sql`
- `seed.sql` or generated seed pipeline
- `queries.sql`
- `benchmark definitions`
- `runner app`
- `README.md`
- `benchmark_results.json`

The SQL should remain conceptually stable even when the implementation language changes.

---

## Success Criteria

The final result should:

- feel like an official DecentDB sample
- prove DecentDB can handle a realistic relational workload
- avoid toy CRUD examples
- support repeatable benchmarking
- support scaling across data sizes
- let developers compare behavior across languages and workloads
- generate meaningful output for docs, demos, and performance discussions

---

## Deliverables

Return the result in this structure:

1. solution summary
2. schema overview
3. ER explanation
4. DDL SQL
5. indexes
6. views
7. triggers
8. seed/data generation design
9. showcase queries
10. benchmark design
11. `{{TARGET_LANGUAGE}}` runner/app
12. CLI/config design
13. README
14. sample benchmark output schema
15. Mermaid ERD

---

## Final Instruction

Optimize for a realistic, repeatable, scalable DecentDB showcase and benchmark suite that can be implemented in different languages by changing only:

- `{{TARGET_LANGUAGE}}`
- `{{SCALE_FACTOR}}`
- `{{BENCHMARK_PROFILE}}`

Do not simplify the project into a generic CRUD sample.
```

---

## Ready-to-Use Example: C# 10x Full

```text
Implement the following specification in C#.

Additional implementation requirements:
- Use idiomatic .NET console app structure.
- Keep the benchmark runner straightforward, readable, and easy to run locally.
- Support scale factor 10x base.
- Support benchmark profile "full".
- Export benchmark results to JSON.
- Keep the database and SQL as the primary focus; do not over-engineer the surrounding application.

Now follow this specification exactly:

[BEGIN SPECIFICATION]
# Task

Create a **DecentDB showcase project** for the domain:

**Incident Response / Operations Command Center**

Implement the surrounding application in:

`C#`

Use a data scale of:

`10x base`

Optional benchmark profile:

`full`

Examples:
- `TARGET_LANGUAGE = C#`, `SCALE_FACTOR = 10`, `BENCHMARK_PROFILE = full`
- `TARGET_LANGUAGE = Dart`, `SCALE_FACTOR = 25`, `BENCHMARK_PROFILE = full`
- `TARGET_LANGUAGE = Rust`, `SCALE_FACTOR = 5`, `BENCHMARK_PROFILE = balanced`

## Primary Goal

Build a polished, realistic, non-e-commerce DecentDB sample that demonstrates:

1. relational schema design
2. meaningful SQL usage
3. realistic seed data generation
4. tunable scale-factor-based data generation
5. repeatable developer-focused benchmarks
6. clear reporting of results

The result should feel like an official sample and benchmark project for DecentDB.

## Domain Model

Model an incident operations system with entities such as:

- teams
- users/responders
- services
- service dependencies
- alert sources
- alerts
- incidents
- incident_services
- incident_responders
- incident_timeline
- deployments
- maintenance_windows
- runbooks
- audit_log

Do not replace this domain with e-commerce, products, carts, storefronts, or generic CRUD examples.

## Schema Requirements

Create a normalized relational schema with about 10–14 tables.

Include:
- primary keys
- foreign keys
- unique constraints
- check constraints
- default values
- generated columns where useful

Include index examples where supported and appropriate:
- standard B-tree indexes
- expression indexes
- partial indexes
- covering indexes
- trigram indexes

Create at least:
- 3 meaningful views
- 2 practical triggers

Do not invent unsupported DecentDB features.

## Data Generation Requirements

Implement a deterministic synthetic data generator.

Use a `SCALE_FACTOR` parameter so that the dataset size can be increased or decreased without changing schema logic.

The generator must:
- produce realistic relational data
- preserve foreign key integrity
- generate believable distributions
- avoid purely uniform random data
- support repeatable runs using a fixed seed

### Base Dataset

Use a base dataset approximately like this:

- teams: 10
- users/responders: 100
- services: 75
- service_dependencies: 150
- alert_sources: 8
- deployments: 500
- alerts: 5,000
- incidents: 1,000
- incident_services: 2,500
- incident_responders: 2,000
- incident_timeline: 10,000
- maintenance_windows: 200
- runbooks: 150

Then scale using `SCALE_FACTOR`.

### Scaling Rules

Do not simply multiply every table by the same number unless justified.

Prefer realistic scaling, for example:
- teams grow slowly
- users and services grow linearly
- alerts grow faster than services
- timeline events grow faster than incidents
- dependencies grow proportionally to service count
- audit log grows from workload execution

Document the scaling rules clearly.

## Benchmark Requirements

Create a benchmark harness that runs repeatable tests and reports clear metrics developers care about.

### Benchmark Categories

At minimum benchmark:

#### 1. Initialization
- schema creation time
- index creation time
- seed data generation time
- data load time
- final database size

#### 2. Insert Performance
- single-row inserts
- batched inserts
- inserts involving foreign keys
- inserts that fire triggers
- inserts affecting indexed/generated-column tables

#### 3. Read Performance
- primary key lookup
- indexed lookup
- range query
- multi-table join
- aggregate report
- CTE report
- supported window function query
- trigram/text search query
- JSON expansion query using `json_each(...)`
- query against a view
- temp table or temp view analysis flow

#### 4. Update Performance
- single-row update
- batched updates
- indexed-column updates
- trigger-firing updates
- workflow-style incident state change update

#### 5. Delete Performance
- single-row delete
- batched cleanup delete
- archive-style delete workflow if appropriate

#### 6. Mixed Workflow Scenarios
Benchmark realistic business workflows, such as:
- create incident with related alerts and responders
- append timeline events during an incident
- resolve incident and write audit entries
- run dashboard query pack
- search incidents and drill into one incident

## Benchmark Measurement Expectations

For each benchmark, capture:

- benchmark name
- query or operation type
- number of iterations
- total elapsed time
- average latency
- median latency if practical
- min/max latency if practical
- rows processed if applicable
- rows returned if applicable
- scale factor used
- final database size if relevant

If practical in `C#`, also capture:
- standard deviation
- p95 latency
- p99 latency

If those are too heavy for the chosen implementation, state that clearly.

## Benchmark Profiles

Support benchmark profiles such as:

### smoke
Minimal and quick:
- reduced iterations
- intended for correctness verification

### balanced
Reasonable benchmark duration:
- moderate iterations
- intended for local developer use

### full
More rigorous:
- more iterations
- suitable for comparing languages / implementations / scales

### stress
Heavy run:
- intended for high-scale testing on powerful machines

Document how each profile changes iteration counts.

## Query and Workload Requirements

Provide at least 15 showcase SQL queries.

These must demonstrate:
- joins
- aggregates
- CTEs
- supported window functions
- view queries
- temp table or temp view usage
- `json_each(...)`
- search patterns
- index-friendly lookups
- reporting queries

For each query, include:
- short description
- what DecentDB feature it demonstrates
- why a developer would care about it

## Language-Specific Application Requirements

Implement the surrounding code in `C#`.

The application should:
- initialize the database
- create schema
- create indexes
- load seed data
- run showcase queries
- run benchmark suite
- print readable results
- optionally export results as JSON or CSV

The code should be simple, idiomatic, and easy to run locally.

Do not let framework complexity overshadow the database and benchmark logic.

## Reporting Requirements

Produce a README that explains:
- project purpose
- why this is a good DecentDB showcase
- schema overview
- scale factor model
- benchmark methodology
- benchmark profiles
- how to run at different scales
- how to compare runs across languages
- known limitations / assumptions

Also produce a machine-readable benchmark output format such as:
- `benchmark_results.json`
- optionally `benchmark_results.csv`

## CLI / Configuration Requirements

Support configuration through command-line args or config file.

At minimum allow:
- target database path
- scale factor
- benchmark profile
- random seed
- whether to rebuild database
- whether to run only schema load
- whether to run only seed/load
- whether to run only showcase queries
- whether to run only benchmarks
- whether to export results

Example desired usage:

- `run --scale 1 --profile smoke`
- `run --scale 10 --profile balanced`
- `run --scale 25 --profile full --export-json`
- `run --scale 10 --profile full --seed 42`

## Suggested Project Layout

Structure the output so it is easy to compare across languages. Prefer something close to:

- `schema.sql`
- `seed.sql` or generated seed pipeline
- `queries.sql`
- `benchmark definitions`
- `runner app`
- `README.md`
- `benchmark_results.json`

The SQL should remain conceptually stable even when the implementation language changes.

## Success Criteria

The final result should:

- feel like an official DecentDB sample
- prove DecentDB can handle a realistic relational workload
- avoid toy CRUD examples
- support repeatable benchmarking
- support scaling across data sizes
- let developers compare behavior across languages and workloads
- generate meaningful output for docs, demos, and performance discussions

## Deliverables

Return the result in this structure:

1. solution summary
2. schema overview
3. ER explanation
4. DDL SQL
5. indexes
6. views
7. triggers
8. seed/data generation design
9. showcase queries
10. benchmark design
11. `C#` runner/app
12. CLI/config design
13. README
14. sample benchmark output schema
15. Mermaid ERD

## Final Instruction

Optimize for a realistic, repeatable, scalable DecentDB showcase and benchmark suite that can be implemented in different languages by changing only:

- `TARGET_LANGUAGE`
- `SCALE_FACTOR`
- `BENCHMARK_PROFILE`

Do not simplify the project into a generic CRUD sample.
[END SPECIFICATION]
```

---

## Notes

- The **wrapper prompt** keeps the implementation language and run profile easy to swap.
- The **core specification** keeps the actual benchmark and schema expectations stable across languages.
- This makes it much easier to compare outputs from multiple coding agents and multiple languages.
