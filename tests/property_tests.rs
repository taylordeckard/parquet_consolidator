use proptest::prelude::*;
use tempfile::TempDir;
use parquet_consolidator::test_utils::*;
use parquet_consolidator::{find_parquet_files, consolidate_parquet_files, is_parquet_file};
use polars::prelude::*;
use std::path::PathBuf;

proptest! {
    #[test]
    fn test_is_parquet_file_with_various_extensions(
        filename in "[a-zA-Z0-9_-]{1,20}",
        extension in prop::sample::select(vec!["parquet", "PARQUET", "Parquet", "txt", "csv", "json", ""])
    ) {
        let path = if extension.is_empty() {
            PathBuf::from(filename)
        } else {
            PathBuf::from(format!("{}.{}", filename, extension))
        };
        
        let result = is_parquet_file(&path);
        let expected = extension.to_lowercase() == "parquet";
        prop_assert_eq!(result, expected);
    }

    #[test]
    fn test_consolidation_preserves_total_row_count(
        num_files in 1usize..=5,
        records_per_file in 1i32..=1000
    ) {
        let temp_dir = TempDir::new().unwrap();
        let test_data_dir = temp_dir.path().join("test_data");
        let output_file = temp_dir.path().join("output.parquet");
        
        std::fs::create_dir_all(&test_data_dir).unwrap();
        
        // Create test files with varying record counts
        let mut total_expected_records = 0;
        for i in 0..num_files {
            let file_path = test_data_dir.join(format!("file_{}.parquet", i));
            let start_id = total_expected_records;
            let end_id = start_id + records_per_file;
            create_test_parquet_file(&file_path, start_id, end_id).unwrap();
            total_expected_records += records_per_file;
        }
        
        // Consolidate files
        let parquet_files = find_parquet_files(&test_data_dir, false).unwrap();
        consolidate_parquet_files(&parquet_files, &output_file, false).unwrap();
        
        // Verify total record count
        let df = LazyFrame::scan_parquet(&output_file, Default::default())?
            .collect()?;
        
        prop_assert_eq!(df.height(), total_expected_records as usize);
    }

    #[test]
    fn test_find_files_respects_recursive_flag(
        depth in 1usize..=3,
        files_per_level in 1usize..=3
    ) {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().join("test_data");
        
        // Create nested directory structure
        create_nested_test_structure(&base_dir, depth, files_per_level).unwrap();
        
        // Test non-recursive
        let non_recursive_files = find_parquet_files(&base_dir, false).unwrap();
        prop_assert_eq!(non_recursive_files.len(), files_per_level);
        
        // Test recursive
        let recursive_files = find_parquet_files(&base_dir, true).unwrap();
        let expected_total = files_per_level * depth;
        prop_assert_eq!(recursive_files.len(), expected_total);
    }
}

fn create_nested_test_structure(
    base_path: &std::path::Path, 
    depth: usize, 
    files_per_level: usize
) -> anyhow::Result<()> {
    std::fs::create_dir_all(base_path)?;
    
    let mut current_path = base_path.to_path_buf();
    let mut file_counter = 0;
    
    for level in 0..depth {
        // Create files at current level
        for i in 0..files_per_level {
            let file_path = current_path.join(format!("file_{}_{}.parquet", level, i));
            create_test_parquet_file(&file_path, file_counter, file_counter + 10)?;
            file_counter += 10;
        }
        
        // Create next level directory if not at the last level
        if level < depth - 1 {
            current_path = current_path.join(format!("level_{}", level + 1));
            std::fs::create_dir_all(&current_path)?;
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod regular_tests {
    use super::*;

    #[test]
    fn test_create_nested_test_structure() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().join("test_data");
        
        create_nested_test_structure(&base_dir, 3, 2).unwrap();
        
        // Verify structure
        assert!(base_dir.join("file_0_0.parquet").exists());
        assert!(base_dir.join("file_0_1.parquet").exists());
        assert!(base_dir.join("level_1").join("file_1_0.parquet").exists());
        assert!(base_dir.join("level_1").join("file_1_1.parquet").exists());
        assert!(base_dir.join("level_1").join("level_2").join("file_2_0.parquet").exists());
        assert!(base_dir.join("level_1").join("level_2").join("file_2_1.parquet").exists());
    }
}
