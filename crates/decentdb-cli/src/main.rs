mod commands;
mod output;
mod repl;

use clap::Parser;

use crate::commands::Cli;

fn main() {
    let cli = Cli::parse();
    std::process::exit(commands::run(cli));
}
