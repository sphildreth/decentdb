# DecentDB RTK command policy

This repository uses RTK to reduce token usage from terminal command output.

At the beginning of terminal-based tasks, run:

- `rtk git status`

Prefer RTK-wrapped commands when output may be large:

- `rtk git status`
- `rtk git diff`
- `rtk git log -n 10`
- `rtk cargo test`
- `rtk cargo test --lib`
- `rtk cargo test --tests`
- `rtk cargo clippy --all-targets --all-features`
- `rtk cargo fmt --check`

For search, prefer scoped RTK/ripgrep searches:

- `rtk rg "pattern" src crates tests benches examples`
- `rtk rg "pattern" -g '!target/' -g '!.git/' -g '!node_modules/' -g '!dist/' -g '!coverage/' .`

Avoid broad recursive searches unless truly needed:

- avoid `rtk grep "pattern" .`
- avoid `rtk find .`

Do not use RTK for shell builtins or shell control flow:

- `cd`
- `test`
- `[ -d ... ]`
- shell loops
- shell conditionals

When reporting terminal work, mention the RTK commands that were used.

If RTK output is too compact to debug a failure, rerun the smallest relevant command normally or inspect the full output path referenced by RTK.
