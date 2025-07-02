use clap::Parser;
use std::path::PathBuf;
use anyhow::Result;
use parquet_consolidator::{find_parquet_files, consolidate_parquet_files};

#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(short, long)]
    input: PathBuf,
    #[arg(short, long)]
    output: PathBuf,
    #[arg(short, long, default_value_t = false)]
    recursive: bool,
    #[arg(short, long, default_value_t = false)]
    verbose: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let parquet_files = find_parquet_files(&args.input, args.recursive)?;

    if parquet_files.is_empty() {
        anyhow::bail!("No parquet files found in the specified directory");
    }

    consolidate_parquet_files(&parquet_files, &args.output, args.verbose)?;

    println!("Successfully consolidated files into {:?}", args.output);
    Ok(())
}
