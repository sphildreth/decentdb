# Troubleshooting

## Common Errors

### `sync peer endpoint must start with http:// or https://`

The endpoint must be a fully qualified HTTP or HTTPS URL.

### `sync peer token_env must not be empty`

Pass a real environment-variable name with `--token-env`.

### `sync scope table '<name>' does not exist`

Create the table before defining the scope.

### `OR is not supported in sync row filters; use AND only`

Rewrite the filter as a conjunction of literal comparisons.

### `schema mismatch for table ...`

The batch was created under a different schema cookie. Reconcile the schema
before retrying.

### `cannot import batch from same replica`

A replica must not import its own exported history.

## Backlog Growth

If the journal keeps growing:

1. run `sync status`
2. run `sync doctor`
3. inspect `sys_sync_peer_lag`
4. look for unresolved conflicts
5. verify that the peer watermark is advancing

## Repeated Replays

If a batch keeps replaying:

- confirm that peer watermarks are being persisted
- inspect `sys_sync_sessions`
- check whether the peer was re-created under a new name

## Version or Capability Mismatch

`sync run` performs a handshake and requires the `batch-envelope-v1`
capability.

If the peer does not advertise that capability, the run fails before any data
moves.

## Journal Issues

If the journal is corrupted or missing:

- run `sync doctor`
- inspect the database file and sidecar journal together
- restore from a known-good copy if necessary

## FAQ

### Why is the default conflict policy conservative?

Because silent merge behavior is the easiest way to lose data without noticing.

### Can I use sync without the HTTP transport?

Yes. Manual export/import works entirely locally.

### Can I use sync on untrusted networks today?

Not as a hardened public endpoint. Use trusted networks or a secure reverse
proxy until transport hardening lands.

