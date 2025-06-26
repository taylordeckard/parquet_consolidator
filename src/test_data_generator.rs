use arrow::array::{Int32Array, StringArray, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::array::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::{File, create_dir_all};
use std::sync::Arc;
use anyhow::Result;

fn main() -> Result<()> {
    // Create test data directory
    create_dir_all("test_data")?;
    
    // Define schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    
    // Create test files
    create_test_file("test_data/file1.parquet", &schema, 0, 100)?;
    create_test_file("test_data/file2.parquet", &schema, 100, 150)?;
    create_test_file("test_data/file3.parquet", &schema, 200, 300)?;
    
    // Create nested directory with more files
    create_dir_all("test_data/nested")?;
    create_test_file("test_data/nested/file4.parquet", &schema, 300, 400)?;
    create_test_file("test_data/nested/file5.parquet", &schema, 400, 500)?;
    
    println!("Created test parquet files in test_data/");
    println!("- file1.parquet (100 records)");
    println!("- file2.parquet (50 records)");
    println!("- file3.parquet (100 records)");
    println!("- nested/file4.parquet (100 records)");
    println!("- nested/file5.parquet (100 records)");
    println!("\nTest consolidation with:");
    println!("cargo run -- -i test_data -o consolidated.parquet");
    println!("cargo run -- -i test_data -o consolidated_recursive.parquet --recursive");
    
    Ok(())
}

fn create_test_file(
    path: &str, 
    schema: &Arc<Schema>, 
    start_id: i32, 
    end_id: i32
) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;
    
    let _count = (end_id - start_id) as usize;
    
    // Create data arrays
    let ids: Vec<i32> = (start_id..end_id).collect();
    let names: Vec<String> = (start_id..end_id)
        .map(|i| format!("name_{}", i))
        .collect();
    let values: Vec<f64> = (start_id..end_id)
        .map(|i| i as f64 * 1.5)
        .collect();
    
    let id_array = Int32Array::from(ids);
    let name_array = StringArray::from(names);
    let value_array = Float64Array::from(values);
    
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(value_array),
        ],
    )?;
    
    writer.write(&batch)?;
    writer.close()?;
    
    Ok(())
}
