pub mod cli;

mod artifacts;
mod baseline;
mod compare;
mod profiles;
mod report;
mod runner;
mod scenarios;
mod storage_inspector;
mod targets;
mod types;

pub fn run(cli: cli::Cli) -> anyhow::Result<()> {
    match cli.command {
        cli::Command::Run(args) => runner::run_command(*args),
        cli::Command::Compare(args) => compare::run_compare_command(args),
        cli::Command::Baseline(args) => baseline::run_baseline_command(args),
        cli::Command::Report(args) => report::run_report_command(args),
        cli::Command::InspectStorage(args) => runner::run_inspect_storage_command(args),
        cli::Command::Internal(args) => runner::run_internal_command(args),
    }
}
