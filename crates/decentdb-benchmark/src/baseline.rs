use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use crate::cli::{BaselineArgs, BaselineCommand, BaselineSetArgs};
use crate::compare::{
    baseline_file_path, baseline_snapshot_from_input, load_comparable_run, BaselineSnapshot,
};

pub(crate) fn run_baseline_command(args: BaselineArgs) -> Result<()> {
    match args.command {
        BaselineCommand::Set(set_args) => run_baseline_set_command(set_args),
    }
}

fn run_baseline_set_command(args: BaselineSetArgs) -> Result<()> {
    let input = load_comparable_run(&args.input)?;
    let baseline = baseline_snapshot_from_input(&args.name, &input)?;
    let output_path = baseline_file_path(&args.artifact_root, &args.name)?;

    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create baseline dir {}", parent.display()))?;
    }

    write_json_file(&output_path, &baseline)?;
    println!("baseline_name={}", args.name);
    println!("baseline={}", output_path.display());
    Ok(())
}

fn write_json_file(path: &Path, baseline: &BaselineSnapshot) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(baseline)?;
    fs::write(path, bytes).with_context(|| format!("write baseline {}", path.display()))
}
