# Security

Sync is local-first, but it still handles credentials and transport endpoints.

## Peer Endpoints

Peer endpoints must use `http://` or `https://`.

The built-in CLI transport does not provide TLS termination, certificate
management, or hardened public-server behavior.

Use it on trusted loopback networks, in local development, or behind a secure
reverse proxy until future transport hardening exists.

## Token Handling

`token_env` stores the environment variable name that contains the secret.
The secret value is read at runtime.

Example:

```bash
export DECENTDB_SYNC_TOKEN='super-secret'
decentdb sync peer add --db=app.ddb --name=central --endpoint=http://127.0.0.1:43123 --token-env=DECENTDB_SYNC_TOKEN
```

## Logging and Redaction

The CLI best-effort redacts the active sync token from failure text before it is
printed. That is useful, but it is not a substitute for secure logging
practices.

Do not rely on logs as a secret store.

## Local Files

The sync journal is a local sidecar file next to the database file. Keep both
files on the same trusted filesystem boundary.

Avoid syncing the journal itself through unrelated tools or backup workflows
unless you know the recovery implications.

## Deferred Hardening

Still deferred:

- built-in TLS server hardening
- certificate rotation
- peer discovery and identity enrollment
- policy-driven secret storage
