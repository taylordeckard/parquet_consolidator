use arrow::array::{Int32Array, StringArray, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::array::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use anyhow::Result;

/// Create a test parquet file with a standard schema
pub fn create_test_parquet_file(path: &Path, start_id: i32, end_id: i32) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    create_test_parquet_file_with_schema(path, &schema, start_id, end_id)
}

/// Create a test parquet file with an extra column for schema compatibility testing
pub fn create_test_parquet_file_with_extra_column(path: &Path, start_id: i32, end_id: i32) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("extra", DataType::Utf8, true), // nullable extra column
    ]));

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;
    
    // Create data arrays
    let ids: Vec<i32> = (start_id..end_id).collect();
    let names: Vec<String> = (start_id..end_id)
        .map(|i| format!("name_{}", i))
        .collect();
    let values: Vec<f64> = (start_id..end_id)
        .map(|i| i as f64 * 1.5)
        .collect();
    let extras: Vec<Option<String>> = (start_id..end_id)
        .map(|i| Some(format!("extra_{}", i)))
        .collect();
    
    let id_array = Int32Array::from(ids);
    let name_array = StringArray::from(names);
    let value_array = Float64Array::from(values);
    let extra_array = StringArray::from(extras);
    
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(value_array),
            Arc::new(extra_array),
        ],
    )?;
    
    writer.write(&batch)?;
    writer.close()?;
    
    Ok(())
}

/// Create a test parquet file with a custom schema
pub fn create_test_parquet_file_with_schema(
    path: &Path, 
    schema: &Arc<Schema>, 
    start_id: i32, 
    end_id: i32
) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;
    
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

/// Create a directory structure with test parquet files
pub fn create_test_directory_structure(base_path: &Path) -> Result<()> {
    std::fs::create_dir_all(base_path)?;
    
    // Create root level files
    create_test_parquet_file(&base_path.join("file1.parquet"), 0, 100)?;
    create_test_parquet_file(&base_path.join("file2.parquet"), 100, 200)?;
    create_test_parquet_file(&base_path.join("file3.parquet"), 200, 300)?;
    
    // Create nested directory
    let nested_dir = base_path.join("nested");
    std::fs::create_dir_all(&nested_dir)?;
    create_test_parquet_file(&nested_dir.join("file4.parquet"), 300, 400)?;
    create_test_parquet_file(&nested_dir.join("file5.parquet"), 400, 500)?;
    
    // Create some non-parquet files to test filtering
    std::fs::write(base_path.join("readme.txt"), "This is not a parquet file")?;
    std::fs::write(nested_dir.join("data.csv"), "id,name,value\n1,test,1.0")?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use polars::prelude::*;

    #[test]
    fn test_create_test_parquet_file() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let test_file = temp_dir.path().join("test.parquet");
        
        create_test_parquet_file(&test_file, 0, 10)?;
        
        // Verify the file was created and contains expected data
        assert!(test_file.exists());
        
        let df = LazyFrame::scan_parquet(&test_file, Default::default())?.collect()?;
        assert_eq!(df.height(), 10);
        assert_eq!(df.width(), 3);
        
        // Check column names
        let columns: Vec<&str> = df.get_column_names();
        assert!(columns.contains(&"id"));
        assert!(columns.contains(&"name"));
        assert!(columns.contains(&"value"));
        
        Ok(())
    }

    #[test]
    fn test_create_test_directory_structure() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let test_dir = temp_dir.path().join("test_data");
        
        create_test_directory_structure(&test_dir)?;
        
        // Verify structure was created
        assert!(test_dir.join("file1.parquet").exists());
        assert!(test_dir.join("file2.parquet").exists());
        assert!(test_dir.join("file3.parquet").exists());
        assert!(test_dir.join("nested").join("file4.parquet").exists());
        assert!(test_dir.join("nested").join("file5.parquet").exists());
        assert!(test_dir.join("readme.txt").exists());
        assert!(test_dir.join("nested").join("data.csv").exists());
        
        Ok(())
    }
}
