#!/usr/bin/env bash

set -e

echo "=== Parquet Consolidator Test Script ==="
echo ""

# Clean up any existing test data
echo "Cleaning up existing test data..."
rm -rf test_data consolidated*.parquet

# Build the project
echo "Building parquet consolidator..."
cargo build --release

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Build successful!"
echo ""

# Generate test data
echo "Generating test data..."
cargo run --bin test_data_generator

echo ""

# Test basic consolidation (non-recursive)
echo "Testing basic consolidation (non-recursive)..."
./target/release/parquet_consolidator -i test_data -o consolidated_basic.parquet -v

if [ -f consolidated_basic.parquet ]; then
    echo "✓ Basic consolidation successful"
    echo "Output file size: $(du -h consolidated_basic.parquet | cut -f1)"
else
    echo "✗ Basic consolidation failed"
    exit 1
fi

echo ""

# Test recursive consolidation
echo "Testing recursive consolidation..."
./target/release/parquet_consolidator -i test_data -o consolidated_recursive.parquet -r -v

if [ -f consolidated_recursive.parquet ]; then
    echo "✓ Recursive consolidation successful"
    echo "Output file size: $(du -h consolidated_recursive.parquet | cut -f1)"
    
    # Compare file sizes (recursive should be larger)
    basic_size=$(stat -f%z consolidated_basic.parquet 2>/dev/null || stat -c%s consolidated_basic.parquet)
    recursive_size=$(stat -f%z consolidated_recursive.parquet 2>/dev/null || stat -c%s consolidated_recursive.parquet)
    
    if [ "$recursive_size" -gt "$basic_size" ]; then
        echo "✓ Recursive consolidation includes more data as expected"
    else
        echo "⚠ Warning: Recursive file is not larger than basic file"
    fi
else
    echo "✗ Recursive consolidation failed"
    exit 1
fi

echo ""

# Test help
echo "Testing help output..."
./target/release/parquet_consolidator --help | head -5

echo ""

# Test error handling (non-existent directory)
echo "Testing error handling with non-existent directory..."
if ./target/release/parquet_consolidator -i non_existent_dir -o error_test.parquet 2>/dev/null; then
    echo "✗ Should have failed with non-existent directory"
    exit 1
else
    echo "✓ Correctly handled non-existent directory error"
fi

echo ""

# Test single file input
echo "Testing single file input..."
./target/release/parquet_consolidator -i test_data/file1.parquet -o single_file_output.parquet -v

if [ -f single_file_output.parquet ]; then
    echo "✓ Single file processing successful"
else
    echo "✗ Single file processing failed"
    exit 1
fi

echo ""
echo "=== All Tests Passed! ==="
echo ""
echo "Generated files:"
ls -la consolidated*.parquet single_file_output.parquet 2>/dev/null || true

echo ""
echo "Usage examples:"
echo "./target/release/parquet_consolidator -i test_data -o output.parquet"
echo "./target/release/parquet_consolidator -i test_data -o output.parquet --recursive --verbose"
echo "./target/release/parquet_consolidator --help"
