use std::process::Command;
use std::path::Path;
use anyhow::{Result, Context};

fn main() -> Result<()> {
    println!("=== Parquet Consolidator Test Suite ===");
    println!();

    // Clean up any existing test data
    cleanup_test_files()?;

    // Build the project
    println!("Building project...");
    build_project()?;
    
    // Generate test data
    println!("Generating test data...");
    generate_test_data()?;

    // Run unit tests
    println!("Running unit tests...");
    run_unit_tests()?;

    // Run integration tests
    println!("Running integration tests...");
    run_integration_tests()?;

    // Run property tests
    println!("Running property-based tests...");
    run_property_tests()?;

    // Run benchmark tests (optional)
    if std::env::args().any(|arg| arg == "--bench") {
        println!("Running benchmark tests...");
        run_benchmark_tests()?;
    }

    println!();
    println!("=== All Tests Passed! ===");
    
    Ok(())
}

fn cleanup_test_files() -> Result<()> {
    println!("Cleaning up existing test data...");
    
    let files_to_remove = [
        "test_data",
        "consolidated_basic.parquet",
        "consolidated_recursive.parquet",
        "single_file_output.parquet",
        "out.parquet",
    ];
    
    for file in &files_to_remove {
        if Path::new(file).exists() {
            if Path::new(file).is_dir() {
                std::fs::remove_dir_all(file)
                    .context(format!("Failed to remove directory: {}", file))?;
            } else {
                std::fs::remove_file(file)
                    .context(format!("Failed to remove file: {}", file))?;
            }
        }
    }
    
    Ok(())
}

fn build_project() -> Result<()> {
    let output = Command::new("cargo")
        .args(&["build", "--release"])
        .output()
        .context("Failed to execute cargo build")?;
    
    if !output.status.success() {
        anyhow::bail!(
            "Build failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    
    println!("✓ Build successful!");
    Ok(())
}

fn generate_test_data() -> Result<()> {
    let output = Command::new("cargo")
        .args(&["run", "--bin", "test_data_generator"])
        .output()
        .context("Failed to generate test data")?;
    
    if !output.status.success() {
        anyhow::bail!(
            "Test data generation failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    
    println!("✓ Test data generated successfully!");
    Ok(())
}

fn run_unit_tests() -> Result<()> {
    let output = Command::new("cargo")
        .args(&["test", "--lib"])
        .output()
        .context("Failed to run unit tests")?;
    
    if !output.status.success() {
        anyhow::bail!(
            "Unit tests failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    
    println!("✓ Unit tests passed!");
    Ok(())
}

fn run_integration_tests() -> Result<()> {
    let output = Command::new("cargo")
        .args(&["test", "--test", "integration_tests"])
        .output()
        .context("Failed to run integration tests")?;
    
    if !output.status.success() {
        anyhow::bail!(
            "Integration tests failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    
    println!("✓ Integration tests passed!");
    Ok(())
}

fn run_property_tests() -> Result<()> {
    let output = Command::new("cargo")
        .args(&["test", "--test", "property_tests"])
        .output()
        .context("Failed to run property tests")?;
    
    if !output.status.success() {
        anyhow::bail!(
            "Property tests failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    
    println!("✓ Property-based tests passed!");
    Ok(())
}

fn run_benchmark_tests() -> Result<()> {
    let output = Command::new("cargo")
        .args(&["test", "--test", "benchmark_tests", "--", "--nocapture"])
        .output()
        .context("Failed to run benchmark tests")?;
    
    if !output.status.success() {
        anyhow::bail!(
            "Benchmark tests failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    
    // Print benchmark output
    println!("{}", String::from_utf8_lossy(&output.stdout));
    println!("✓ Benchmark tests completed!");
    Ok(())
}
