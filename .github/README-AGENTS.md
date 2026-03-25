# DecentDB Agent Customizations

This directory contains workspace-level customizations for coding agents working in DecentDB.

Use these files for different scopes of guidance.

## Quick Decision Guide

Use the always-on Rust instructions when:

- you are editing Rust files and want repository rules to apply automatically
- you need baseline constraints around safety, ABI stability, ownership, testing, and dependency discipline

Use the Rust skill when:

- the task is specifically about generating, refactoring, reviewing, or debugging Rust code
- you want a deeper workflow with references for errors, async, FFI, performance, and testing

Use a Rust prompt when:

- you want a focused entry point for one kind of task such as implementation, review, or debugging
- you want to steer the agent into a specific workflow immediately from chat

## What Lives Where

### Always-On Instructions

- [instructions/rust.instructions.md](./instructions/rust.instructions.md)

This file applies to `**/*.rs` and gives repository-specific defaults for Rust work.

It is the right place for rules that should always be active during Rust edits.

### Rust Skill

- [skills/rust-code-generation/SKILL.md](./skills/rust-code-generation/SKILL.md)

This is the on-demand Rust workflow.

It is the right place for:

- implementation and refactoring guidance
- review guidance
- validation expectations
- references that should load only when relevant

Skill references:

- [skills/rust-code-generation/references/errors.md](./skills/rust-code-generation/references/errors.md)
- [skills/rust-code-generation/references/async.md](./skills/rust-code-generation/references/async.md)
- [skills/rust-code-generation/references/ffi.md](./skills/rust-code-generation/references/ffi.md)
- [skills/rust-code-generation/references/performance.md](./skills/rust-code-generation/references/performance.md)
- [skills/rust-code-generation/references/testing.md](./skills/rust-code-generation/references/testing.md)

### Rust Prompts

- [prompts/implement-rust-feature.prompt.md](./prompts/implement-rust-feature.prompt.md)
- [prompts/review-rust-change.prompt.md](./prompts/review-rust-change.prompt.md)
- [prompts/debug-rust-failure.prompt.md](./prompts/debug-rust-failure.prompt.md)

Use these when you want a task-specific slash-command style entry point.

Prompt selection:

- `Implement Rust Feature`: make or refactor a Rust change
- `Review Rust Change`: review Rust code for bugs, regressions, and validation gaps
- `Debug Rust Failure`: fix compiler errors, borrow checker problems, clippy failures, or failing tests

## How They Work Together

For normal Rust editing:

1. The Rust instructions file auto-applies because the target file matches `**/*.rs`.
2. If the task clearly matches Rust code generation or review work, the Rust skill can also load.
3. If you start from a prompt, the prompt gives the agent a focused workflow and points it at the Rust skill and references.

That means:

- instructions provide baseline guardrails
- the skill provides the deeper Rust workflow
- prompts provide task-shaped entry points

## Related Repository Rules

These files still remain authoritative for broader repository behavior:

- [copilot-instructions.md](./copilot-instructions.md)
- [../AGENTS.md](../AGENTS.md)

Use those for repository-wide engineering standards. Use the Rust customizations here for Rust-specific agent behavior.