use clap::Parser;

#[derive(Parser)]
#[command(name = "decentdb")]
#[command(about = "DecentDB Command Line Interface")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Print engine version
    Version,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Version => {
            println!("DecentDB version: {}", decentdb::version());
        }
    }

    Ok(())
}
