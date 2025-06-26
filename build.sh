#!/usr/bin/env bash

# Example script to demonstrate the parquet consolidator

echo "Building parquet consolidator..."
cargo build --release

if [ $? -eq 0 ]; then
    echo "Build successful!"
    echo ""
    echo "Example usage:"
    echo "./target/release/parquet_consolidator -i /path/to/parquet/files -o consolidated.parquet"
    echo ""
    echo "For help:"
    echo "./target/release/parquet_consolidator --help"
else
    echo "Build failed!"
    exit 1
fi
