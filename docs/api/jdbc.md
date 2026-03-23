# Java Smoke Coverage

Phase 4 currently validates Java against the stable C ABI with a direct FFM smoke test rather than a packaged JDBC driver.

File:

```text
tests/bindings/java/Smoke.java
```

It proves:
- library load
- database open
- one write
- one read
- one error path

## Run locally

```bash
cargo build -p decentdb
javac tests/bindings/java/Smoke.java
java --enable-native-access=ALL-UNNAMED -cp tests/bindings/java Smoke
```
