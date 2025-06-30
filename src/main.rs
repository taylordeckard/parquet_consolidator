use clap::Parser;
use std::path::PathBuf;
use anyhow::{Result, Context};
use walkdir::WalkDir;
use arrow::datatypes::Schema;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(about = "A tool to consolidate multiple parquet files into a single parquet file")]
struct Args {
    /// Input directory path containing parquet files
    #[arg(short, long)]
    input: PathBuf,
    
    /// Output parquet file path
    #[arg(short, long)]
    output: PathBuf,
    
    /// Recursively search subdirectories
    #[arg(short, long, default_value_t = false)]
    recursive: bool,
    
    /// Verbose output
    #[arg(short, long, default_value_t = false)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    if args.verbose {
        println!("Starting parquet consolidation...");
        println!("Input path: {:?}", args.input);
        println!("Output path: {:?}", args.output);
        println!("Recursive: {}", args.recursive);
    }
    
    // Find all parquet files
    let parquet_files = find_parquet_files(&args.input, args.recursive)?;
    
    if parquet_files.is_empty() {
        anyhow::bail!("No parquet files found in the specified directory");
    }
    
    if args.verbose {
        println!("Found {} parquet files:", parquet_files.len());
        for file in &parquet_files {
            println!("  - {:?}", file);
        }
    }
    
    // Consolidate parquet files
    consolidate_parquet_files(&parquet_files, &args.output, args.verbose)?;
    
    println!("Successfully consolidated {} parquet files into {:?}", 
             parquet_files.len(), args.output);
    
    Ok(())
}

fn find_parquet_files(input_path: &PathBuf, recursive: bool) -> Result<Vec<PathBuf>> {
    let mut parquet_files = Vec::new();
    
    if !input_path.exists() {
        anyhow::bail!("Input path does not exist: {:?}", input_path);
    }
    
    if input_path.is_file() {
        // If input is a single file, check if it's a parquet file
        if is_parquet_file(input_path) {
            parquet_files.push(input_path.clone());
        } else {
            anyhow::bail!("Input file is not a parquet file: {:?}", input_path);
        }
    } else if input_path.is_dir() {
        // Search directory for parquet files
        let walker = if recursive {
            WalkDir::new(input_path)
        } else {
            WalkDir::new(input_path).max_depth(1)
        };
        
        for entry in walker.into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() && is_parquet_file(entry.path()) {
                parquet_files.push(entry.path().to_path_buf());
            }
        }
    }
    
    parquet_files.sort();
    Ok(parquet_files)
}

fn is_parquet_file(path: &std::path::Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_lowercase() == "parquet")
        .unwrap_or(false)
}

fn consolidate_parquet_files(
    input_files: &[PathBuf], 
    output_path: &PathBuf,
    verbose: bool
) -> Result<()> {
    if input_files.is_empty() {
        anyhow::bail!("No input files provided");
    }
    
    // Read the first file to get the schema and validate all schemas upfront
    let schema = get_and_validate_schema(input_files, verbose)?;
    
    // Create output file
    let output_file = File::create(output_path)
        .context(format!("Failed to create output file: {:?}", output_path))?;
    
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_write_batch_size(8192) // Optimize batch size
        .build();
    
    let mut writer = ArrowWriter::try_new(output_file, schema.clone(), Some(props))?;
    let mut total_rows = 0;
    
    // Process each input file
    for (idx, input_file) in input_files.iter().enumerate() {
        if verbose {
            println!("Processing file {}/{}: {:?}", idx + 1, input_files.len(), input_file);
        }
        
        let rows_written = process_single_file(input_file, &mut writer, verbose)?;
        total_rows += rows_written;
        
        if verbose {
            println!("  Wrote {} rows from this file", rows_written);
        }
    }
    
    writer.close()?;
    
    if verbose {
        println!("Total rows written: {}", total_rows);
    }
    
    Ok(())
}

fn get_and_validate_schema(input_files: &[PathBuf], verbose: bool) -> Result<Arc<Schema>> {
    // Read the first file to get the schema
    let first_file = File::open(&input_files[0])
        .context(format!("Failed to open first parquet file: {:?}", input_files[0]))?;
    
    let builder = ParquetRecordBatchReaderBuilder::try_new(first_file)?;
    let schema = builder.schema().clone();
    
    if verbose {
        println!("Schema from first file: {:?}", schema);
        println!("Validating schema compatibility across all files...");
    }
    
    // Validate all schemas upfront to fail fast if there are incompatibilities
    for (idx, input_file) in input_files.iter().skip(1).enumerate() {
        let file = File::open(input_file)
            .context(format!("Failed to open parquet file: {:?}", input_file))?;
        
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let file_schema = builder.schema();
        
        if !schemas_compatible(&schema, file_schema) {
            anyhow::bail!(
                "Schema mismatch in file {:?} (file {}/{}). Expected: {:?}, Found: {:?}",
                input_file, idx + 2, input_files.len(), schema, file_schema
            );
        }
    }
    
    if verbose {
        println!("All schemas are compatible!");
    }
    
    Ok(schema)
}

fn process_single_file(
    input_file: &PathBuf, 
    writer: &mut ArrowWriter<File>,
    verbose: bool
) -> Result<usize> {
    let file = File::open(input_file)
        .context(format!("Failed to open parquet file: {:?}", input_file))?;
    
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    
    let mut rows_written = 0;
    let mut batch_count = 0;
    
    for batch_result in reader {
        let batch = batch_result
            .context(format!("Failed to read batch from file: {:?}", input_file))?;
        
        rows_written += batch.num_rows();
        batch_count += 1;
        
        writer.write(&batch)
            .context(format!("Failed to write batch {} from file: {:?}", batch_count, input_file))?;
    }
    
    if verbose && batch_count > 1 {
        println!("  Processed {} batches", batch_count);
    }
    
    Ok(rows_written)
}

fn schemas_compatible(schema1: &Schema, schema2: &Schema) -> bool {
    // Check if schemas are compatible (same fields, same types)
    if schema1.fields().len() != schema2.fields().len() {
        return false;
    }
    
    for (field1, field2) in schema1.fields().iter().zip(schema2.fields().iter()) {
        if field1.name() != field2.name() || field1.data_type() != field2.data_type() {
            return false;
        }
    }
    
    true
}
