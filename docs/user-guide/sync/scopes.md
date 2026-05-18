# Scopes

Scopes are the main way to make sync local-first and selective.

They let you say: "replicate these tables, and only the rows that match this
predicate."

## Scope Rules

The current scope validator is intentionally strict:

- `include_tables` must not be empty.
- Every table must exist.
- Every included table must have a stable primary key.
- The row filter is optional.
- When present, the row filter must use `AND` only.
- Each comparison must be a single unqualified column against a literal.
- The referenced columns must exist on every included table.
- The referenced columns must be part of each table's primary key.

## Valid Examples

```bash
# One tenant replicated across two tables.
decentdb sync scope create \
  --db=app.ddb \
  --name=tenant_42 \
  --include=accounts,orders \
  --row-filter="tenant_id = 42"

# A more selective scope for a shard key.
decentdb sync scope create \
  --db=app.ddb \
  --name=tenant_42_readers \
  --include=accounts,orders,audit_events \
  --row-filter="tenant_id = 42 AND shard_id = 7"

# Multiple literal values are allowed.
decentdb sync scope create \
  --db=app.ddb \
  --name=tenant_42_or_43 \
  --include=accounts \
  --row-filter="tenant_id IN (42, 43)"
```

## Invalid Examples

These fail validation today:

```bash
# OR is not allowed.
decentdb sync scope create --db=app.ddb --name=bad --include=accounts --row-filter="tenant_id = 42 OR tenant_id = 43"

# Functions are not allowed.
decentdb sync scope create --db=app.ddb --name=bad --include=accounts --row-filter="lower(email) = 'a@example.com'"

# Dotted column references are not allowed.
decentdb sync scope create --db=app.ddb --name=bad --include=accounts --row-filter="accounts.tenant_id = 42"

# Parameters are not allowed.
decentdb sync scope create --db=app.ddb --name=bad --include=accounts --row-filter="tenant_id = ?"

# Columns must be part of the PK for every included table.
decentdb sync scope create --db=app.ddb --name=bad --include=accounts,orders --row-filter="status = 'open'"
```

## Tenant Pattern

For a per-tenant app, a common shape is:

- `accounts` table keyed by `(tenant_id, account_id)`
- `orders` table keyed by `(tenant_id, order_id)`
- `messages` table keyed by `(tenant_id, message_id)`

Then a scope such as `tenant_id = 42` can safely move all rows for one tenant.

## User Pattern

For a per-user workspace:

- `documents` keyed by `(user_id, document_id)`
- `document_acl` keyed by `(user_id, document_id, principal_id)`
- `activity_feed` keyed by `(user_id, event_id)`

Use a filter such as `user_id = 7`.

## Bindings

Scopes are only active for a peer after you bind that peer to the scope:

```bash
decentdb sync scope create --db=app.ddb --name=tenant_42 --include=accounts,orders --row-filter="tenant_id = 42"
decentdb sync scope bind --db=app.ddb --peer=central --scope=tenant_42
decentdb sync scope bindings --db=app.ddb --format=table
```
