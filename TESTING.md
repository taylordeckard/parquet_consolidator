# Parquet Consolidator Testing

This project uses comprehensive Rust unit and integration testing.

## Test Structure

### Unit Tests
Located in `src/consolidator.rs` and `src/test_utils.rs`:
- Test individual functions in isolation
- Test edge cases and error conditions
- Test file discovery and schema validation

### Integration Tests
Located in `tests/integration_tests.rs`:
- Test CLI functionality end-to-end
- Test command-line argument parsing
- Test error handling for invalid inputs

### Property-Based Tests
Located in `tests/property_tests.rs`:
- Use `proptest` crate for fuzzing inputs
- Test that consolidation preserves total row count
- Test recursive vs non-recursive file discovery
- Test with randomly generated file structures

### Benchmark Tests
Located in `tests/benchmark_tests.rs`:
- Measure performance with large datasets
- Test memory usage patterns
- Provide timing information for optimization

## Running Tests

### Quick Test Suite
```bash
cargo run --bin test_runner
```

### Complete Test Suite with Benchmarks
```bash
cargo run --bin test_runner -- --bench
```

### Individual Test Categories
```bash
# Unit tests only
cargo test --lib

# Integration tests only
cargo test --test integration_tests

# Property-based tests only
cargo test --test property_tests

# Benchmark tests only
cargo test --test benchmark_tests -- --nocapture
```

### Documentation Tests
```bash
cargo test --doc
```

## Test Utilities

The `test_utils` module provides:
- Functions to create test parquet files with various schemas
- Directory structure creation for testing
- Helper functions for setting up test scenarios

## Benefits Over Shell Testing

1. **Type Safety**: Rust's type system catches errors at compile time
2. **Better Error Messages**: Detailed error reporting and stack traces
3. **Property-Based Testing**: Automatically tests edge cases
4. **Performance Metrics**: Built-in timing and memory usage analysis
5. **IDE Integration**: Full debugging and breakpoint support
6. **Parallel Execution**: Tests run in parallel by default
7. **Cross-Platform**: Works consistently across different operating systems

## Test Data Generation

Test data is generated programmatically using the Arrow libraries, ensuring:
- Consistent schemas across test files
- Predictable data for assertions
- Various file sizes for performance testing
- Nested directory structures for recursive testing
