# RTK rules for KiloCode

Use RTK-wrapped commands when running common development commands so shell output is compressed before it reaches the model context.

Prefer `rtk <command>` for commands such as:

- `git status`
- `git diff`
- `git log`
- `cargo test`
- `cargo clippy`
- `cargo fmt --check`
- `rg`
- `grep`
- `find`

Do not wrap shell builtins, directory changes, or conditionals.

If RTK hides useful failure details, rerun the narrowest necessary command without RTK.
