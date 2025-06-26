#!/usr/bin/env bash

# Example usage of the parquet consolidator

echo "=== Parquet Consolidator Usage Examples ==="
echo

# Build the project first
echo "1. Building the project..."
cargo build --release
echo

# Generate test data
echo "2. Generating test data..."
cargo run --bin test_data_generator
echo

# Basic usage examples
echo "3. Basic consolidation (non-recursive):"
echo "   ./target/release/parquet_consolidator -i test_data -o basic_output.parquet -v"
./target/release/parquet_consolidator -i test_data -o basic_output.parquet -v
echo

echo "4. Recursive consolidation (includes subdirectories):"
echo "   ./target/release/parquet_consolidator -i test_data -o recursive_output.parquet -r -v"
./target/release/parquet_consolidator -i test_data -o recursive_output.parquet -r -v
echo

echo "5. Single file processing:"
echo "   ./target/release/parquet_consolidator -i test_data/file1.parquet -o single_output.parquet -v"
./target/release/parquet_consolidator -i test_data/file1.parquet -o single_output.parquet -v
echo

echo "Generated output files:"
ls -la *_output.parquet 2>/dev/null || echo "No output files found"

echo
echo "=== Usage Summary ==="
echo "The parquet consolidator can:"
echo "- Consolidate multiple parquet files into a single file"
echo "- Process files recursively in subdirectories"
echo "- Validate schema compatibility between files"
echo "- Handle single file processing"
echo "- Provide verbose output for debugging"
echo
echo "For more help: ./target/release/parquet_consolidator --help"
