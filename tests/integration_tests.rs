use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;
use parquet_consolidator::test_utils::*;

#[test]
fn test_cli_help() {
    let mut cmd = Command::cargo_bin("parquet_consolidator").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage:"));
}

#[test]
fn test_cli_basic_consolidation() {
    let temp_dir = TempDir::new().unwrap();
    let test_data_dir = temp_dir.path().join("test_data");
    let output_file = temp_dir.path().join("output.parquet");
    
    // Create test data
    create_test_directory_structure(&test_data_dir).unwrap();
    
    let mut cmd = Command::cargo_bin("parquet_consolidator").unwrap();
    cmd.arg("-i")
        .arg(&test_data_dir)
        .arg("-o")
        .arg(&output_file)
        .arg("-v")
        .assert()
        .success()
        .stdout(predicate::str::contains("Successfully consolidated"));
    
    // Verify output file was created
    assert!(output_file.exists());
}

#[test]
fn test_cli_recursive_consolidation() {
    let temp_dir = TempDir::new().unwrap();
    let test_data_dir = temp_dir.path().join("test_data");
    let output_file = temp_dir.path().join("output.parquet");
    
    // Create test data
    create_test_directory_structure(&test_data_dir).unwrap();
    
    let mut cmd = Command::cargo_bin("parquet_consolidator").unwrap();
    cmd.arg("-i")
        .arg(&test_data_dir)
        .arg("-o")
        .arg(&output_file)
        .arg("--recursive")
        .arg("--verbose")
        .assert()
        .success()
        .stdout(predicate::str::contains("Successfully consolidated"));
    
    // Verify output file was created
    assert!(output_file.exists());
}

#[test]
fn test_cli_single_file_input() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.parquet");
    let output_file = temp_dir.path().join("output.parquet");
    
    // Create a single test file
    create_test_parquet_file(&input_file, 0, 100).unwrap();
    
    let mut cmd = Command::cargo_bin("parquet_consolidator").unwrap();
    cmd.arg("-i")
        .arg(&input_file)
        .arg("-o")
        .arg(&output_file)
        .assert()
        .success()
        .stdout(predicate::str::contains("Successfully consolidated"));
    
    // Verify output file was created
    assert!(output_file.exists());
}

#[test]
fn test_cli_nonexistent_input() {
    let temp_dir = TempDir::new().unwrap();
    let nonexistent = temp_dir.path().join("nonexistent");
    let output_file = temp_dir.path().join("output.parquet");
    
    let mut cmd = Command::cargo_bin("parquet_consolidator").unwrap();
    cmd.arg("-i")
        .arg(&nonexistent)
        .arg("-o")
        .arg(&output_file)
        .assert()
        .failure()
        .stderr(predicate::str::contains("No parquet files found"));
}

#[test]
fn test_cli_invalid_file_type() {
    let temp_dir = TempDir::new().unwrap();
    let text_file = temp_dir.path().join("test.txt");
    let output_file = temp_dir.path().join("output.parquet");
    
    // Create a non-parquet file
    fs::write(&text_file, "This is not a parquet file").unwrap();
    
    let mut cmd = Command::cargo_bin("parquet_consolidator").unwrap();
    cmd.arg("-i")
        .arg(&text_file)
        .arg("-o")
        .arg(&output_file)
        .assert()
        .failure()
        .stderr(predicate::str::contains("not a parquet file"));
}

#[test]
fn test_cli_empty_directory() {
    let temp_dir = TempDir::new().unwrap();
    let empty_dir = temp_dir.path().join("empty");
    let output_file = temp_dir.path().join("output.parquet");
    
    // Create empty directory
    fs::create_dir(&empty_dir).unwrap();
    
    let mut cmd = Command::cargo_bin("parquet_consolidator").unwrap();
    cmd.arg("-i")
        .arg(&empty_dir)
        .arg("-o")
        .arg(&output_file)
        .assert()
        .failure()
        .stderr(predicate::str::contains("No parquet files found"));
}

#[test]
fn test_cli_missing_arguments() {
    let mut cmd = Command::cargo_bin("parquet_consolidator").unwrap();
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}
