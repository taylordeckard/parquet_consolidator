# Parquet Consolidator

A Rust command-line tool that consolidates multiple parquet files into a single parquet file.

## Features

- Consolidate multiple parquet files from a directory into a single file
- Recursive directory scanning option
- Schema validation to ensure compatibility between files
- Verbose output for detailed processing information
- Support for both single file and directory input

## Installation

### Prerequisites

- Rust (1.70 or later)
- Cargo

### Build from source

```bash
git clone <repository-url>
cd parquet_consolidator
cargo build --release
```

The binary will be available at `target/release/parquet_consolidator`.

## Usage

### Basic usage

```bash
# Consolidate all parquet files in a directory
parquet_consolidator -i /path/to/input/directory -o /path/to/output.parquet

# Recursively search subdirectories
parquet_consolidator -i /path/to/input/directory -o /path/to/output.parquet --recursive

# Verbose output
parquet_consolidator -i /path/to/input/directory -o /path/to/output.parquet --verbose

# Process a single file (useful for validation)
parquet_consolidator -i /path/to/single/file.parquet -o /path/to/output.parquet
```

### Command-line options

- `-i, --input <PATH>`: Input directory path containing parquet files (required)
- `-o, --output <PATH>`: Output parquet file path (required)
- `-r, --recursive`: Recursively search subdirectories (optional)
- `-v, --verbose`: Enable verbose output (optional)
- `-h, --help`: Show help information
- `-V, --version`: Show version information

### Examples

```bash
# Basic consolidation
parquet_consolidator -i ./data -o consolidated.parquet

# Recursive with verbose output
parquet_consolidator -i ./data -o consolidated.parquet -r -v

# Short form options
parquet_consolidator -i ./data -o output.parquet -rv
```

## How it works

1. **File Discovery**: The tool scans the input directory (and subdirectories if `--recursive` is specified) for files with `.parquet` extension.

2. **Schema Validation**: It reads the schema from the first parquet file and validates that all other files have compatible schemas (same field names and data types).

3. **Consolidation**: The tool reads data from each parquet file in batches and writes them to the output file, maintaining the original schema and data integrity.

4. **Error Handling**: The tool provides clear error messages for common issues like missing files, schema mismatches, or I/O errors.

## Requirements

- All parquet files must have compatible schemas (same column names and data types)
- Sufficient disk space for the consolidated output file
- Read permissions for input files and write permissions for output location

## Error Handling

The tool handles various error conditions gracefully:

- **No parquet files found**: Exits with an error if no `.parquet` files are found in the input directory
- **Schema mismatch**: Exits with an error if parquet files have incompatible schemas
- **File I/O errors**: Provides clear error messages for file access issues
- **Invalid paths**: Validates input and output paths before processing

## Performance Considerations

- The tool processes files in batches to manage memory usage efficiently
- Files are processed sequentially to avoid excessive memory consumption
- Schema validation is performed upfront to fail fast on incompatible data

## License

This project is licensed under the MIT License - see the LICENSE file for details.
