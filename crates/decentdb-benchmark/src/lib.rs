pub mod cli;

mod profiles;
mod runner;
mod scenarios;
mod types;

pub fn run(cli: cli::Cli) -> anyhow::Result<()> {
    match cli.command {
        cli::Command::Run(args) => runner::run_command(args),
    }
}
