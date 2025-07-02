use std::time::Instant;
use tempfile::TempDir;
use parquet_consolidator::test_utils::*;
use parquet_consolidator::{find_parquet_files, consolidate_parquet_files};
use polars::prelude::*;
use anyhow::Result;

#[test]
fn benchmark_basic_consolidation() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let test_data_dir = temp_dir.path().join("test_data");
    let output_file = temp_dir.path().join("output.parquet");
    
    // Create larger test dataset
    create_large_test_dataset(&test_data_dir, 10, 10000).unwrap();
    
    let start = Instant::now();
    
    let parquet_files = find_parquet_files(&test_data_dir, false).unwrap();
    consolidate_parquet_files(&parquet_files, &output_file, false).unwrap();
    
    let duration = start.elapsed();
    
    println!("Consolidation of {} files with 10k records each took: {:?}", 
             parquet_files.len(), duration);
    
    // Verify the result
    assert!(output_file.exists());
    
    let df = LazyFrame::scan_parquet(&output_file, Default::default())?
        .collect()?;
    assert_eq!(df.height(), 100_000); // 10 files * 10k records
    
    Ok(())
}

#[test]
fn test_memory_usage_large_files() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let test_data_dir = temp_dir.path().join("test_data");
    let output_file = temp_dir.path().join("output.parquet");
    
    // Create a few large files instead of many small ones
    create_large_test_dataset(&test_data_dir, 3, 50000).unwrap();
    
    let parquet_files = find_parquet_files(&test_data_dir, false).unwrap();
    let result = consolidate_parquet_files(&parquet_files, &output_file, false);
    
    assert!(result.is_ok());
    assert!(output_file.exists());
    
    let df = LazyFrame::scan_parquet(&output_file, Default::default())?
        .collect()?;
    assert_eq!(df.height(), 150_000); // 3 files * 50k records
    
    Ok(())
}

fn create_large_test_dataset(base_path: &std::path::Path, num_files: usize, records_per_file: i32) -> Result<()> {
    std::fs::create_dir_all(base_path)?;
    
    for i in 0..num_files {
        let file_path = base_path.join(format!("large_file_{}.parquet", i));
        let start_id = i as i32 * records_per_file;
        let end_id = start_id + records_per_file;
        create_test_parquet_file(&file_path, start_id, end_id)?;
    }
    
    Ok(())
}
