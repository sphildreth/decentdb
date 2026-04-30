mod commands;
mod output;
mod repl;

use clap::Parser;

use crate::commands::Cli;

// ADR 0139: opt-in low-fragmentation allocator for the CLI binary.
// Library consumers (bindings, embedders) remain free to choose their own
// `#[global_allocator]`; this only takes effect when the `mimalloc` feature
// is enabled at build time.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let cli = Cli::parse();
    std::process::exit(commands::run(cli));
}
