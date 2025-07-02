use std::path::PathBuf;
use anyhow::{Result, Context};
use walkdir::WalkDir;
use polars::prelude::*;
use std::fs::File;

/// Find all parquet files in the given path
/// 
/// # Examples
/// 
/// ```
/// use std::path::PathBuf;
/// use parquet_consolidator::find_parquet_files;
/// use tempfile::TempDir;
/// use parquet_consolidator::test_utils::create_test_parquet_file;
/// 
/// let temp_dir = TempDir::new().unwrap();
/// let test_file = temp_dir.path().join("test.parquet");
/// create_test_parquet_file(&test_file, 0, 10).unwrap();
/// 
/// let files = find_parquet_files(&test_file, false).unwrap();
/// assert_eq!(files.len(), 1);
/// ```
pub fn find_parquet_files(input_path: &PathBuf, recursive: bool) -> Result<Vec<PathBuf>> {
    let mut parquet_files = Vec::new();

    if input_path.is_file() {
        if is_parquet_file(input_path) {
            parquet_files.push(input_path.clone());
        } else {
            anyhow::bail!("Input file is not a parquet file: {:?}", input_path);
        }
    } else if input_path.is_dir() {
        let walker = if recursive {
            WalkDir::new(input_path)
        } else {
            WalkDir::new(input_path).max_depth(1)
        };

        for entry in walker.into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() && is_parquet_file(entry.path()) {
                parquet_files.push(entry.path().to_path_buf());
            }
        }
    }

    Ok(parquet_files)
}

/// Check if a file has a parquet extension
/// 
/// # Examples
/// 
/// ```
/// use parquet_consolidator::is_parquet_file;
/// use std::path::Path;
/// 
/// assert!(is_parquet_file(Path::new("data.parquet")));
/// assert!(is_parquet_file(Path::new("DATA.PARQUET")));
/// assert!(!is_parquet_file(Path::new("data.csv")));
/// ```
pub fn is_parquet_file(path: &std::path::Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_lowercase() == "parquet")
        .unwrap_or(false)
}

/// Consolidate multiple parquet files into a single file
/// 
/// # Examples
/// 
/// ```no_run
/// use std::path::PathBuf;
/// use parquet_consolidator::consolidate_parquet_files;
/// 
/// let input_files = vec![
///     PathBuf::from("file1.parquet"),
///     PathBuf::from("file2.parquet"),
/// ];
/// let output_path = PathBuf::from("consolidated.parquet");
/// 
/// consolidate_parquet_files(&input_files, &output_path, true).unwrap();
/// ```
pub fn consolidate_parquet_files(input_files: &[PathBuf], output_path: &PathBuf, verbose: bool) -> Result<()> {
    if input_files.is_empty() {
        anyhow::bail!("No input files provided");
    }

    let mut dfs = Vec::new();

    for input_file in input_files {
        if verbose {
            println!("Reading file: {:?}", input_file);
        }

        let df = LazyFrame::scan_parquet(input_file.to_str().unwrap(), Default::default())?;
        dfs.push(df);
    }

    let union_args = UnionArgs { parallel: true, rechunk: true, to_supertypes: true };
    let mut concat_df = concat(dfs, union_args)
        .context("Failed to concatenate DataFrames")?
        .collect()
        .context("Failed to execute lazy computation")?;

    if verbose {
        println!("Writing consolidated parquet file to {:?}", output_path);
    }

    let file = File::create(output_path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut concat_df)
        .context("Failed to write consolidated parquet file")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_is_parquet_file() {
        // Test valid parquet files
        assert!(is_parquet_file(std::path::Path::new("test.parquet")));
        assert!(is_parquet_file(std::path::Path::new("test.PARQUET")));
        assert!(is_parquet_file(std::path::Path::new("/path/to/file.parquet")));
        
        // Test invalid files
        assert!(!is_parquet_file(std::path::Path::new("test.txt")));
        assert!(!is_parquet_file(std::path::Path::new("test.csv")));
        assert!(!is_parquet_file(std::path::Path::new("test")));
        assert!(!is_parquet_file(std::path::Path::new("test.")));
    }

    #[test]
    fn test_find_parquet_files_single_file() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let test_file = temp_dir.path().join("test.parquet");
        
        // Create a simple test parquet file
        create_test_parquet_file(&test_file, 0, 10)?;
        
        let result = find_parquet_files(&test_file, false)?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], test_file);
        
        Ok(())
    }

    #[test]
    fn test_find_parquet_files_non_parquet_file() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.txt");
        
        // Create an empty file for testing
        fs::write(&test_file, "").unwrap();
        
        let result = find_parquet_files(&test_file, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not a parquet file"));
    }

    #[test]
    fn test_find_parquet_files_directory_non_recursive() -> Result<()> {
        let temp_dir = TempDir::new()?;
        
        // Create test files
        create_test_parquet_file(&temp_dir.path().join("file1.parquet"), 0, 5)?;
        create_test_parquet_file(&temp_dir.path().join("file2.parquet"), 5, 10)?;
        fs::write(temp_dir.path().join("file3.txt"), "")?;
        
        // Create subdirectory with parquet file
        let sub_dir = temp_dir.path().join("subdir");
        fs::create_dir(&sub_dir)?;
        create_test_parquet_file(&sub_dir.join("file4.parquet"), 10, 15)?;
        
        let result = find_parquet_files(&temp_dir.path().to_path_buf(), false)?;
        
        // Should find only the 2 parquet files in the root directory
        assert_eq!(result.len(), 2);
        
        let file_names: Vec<String> = result
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        
        assert!(file_names.contains(&"file1.parquet".to_string()));
        assert!(file_names.contains(&"file2.parquet".to_string()));
        assert!(!file_names.contains(&"file4.parquet".to_string()));
        
        Ok(())
    }

    #[test]
    fn test_find_parquet_files_directory_recursive() -> Result<()> {
        let temp_dir = TempDir::new()?;
        
        // Create test files
        create_test_parquet_file(&temp_dir.path().join("file1.parquet"), 0, 5)?;
        create_test_parquet_file(&temp_dir.path().join("file2.parquet"), 5, 10)?;
        
        // Create subdirectory with parquet file
        let sub_dir = temp_dir.path().join("subdir");
        fs::create_dir(&sub_dir)?;
        create_test_parquet_file(&sub_dir.join("file3.parquet"), 10, 15)?;
        
        // Create nested subdirectory with parquet file
        let nested_dir = sub_dir.join("nested");
        fs::create_dir(&nested_dir)?;
        create_test_parquet_file(&nested_dir.join("file4.parquet"), 15, 20)?;
        
        let result = find_parquet_files(&temp_dir.path().to_path_buf(), true)?;
        
        // Should find all 4 parquet files
        assert_eq!(result.len(), 4);
        
        let file_names: Vec<String> = result
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        
        assert!(file_names.contains(&"file1.parquet".to_string()));
        assert!(file_names.contains(&"file2.parquet".to_string()));
        assert!(file_names.contains(&"file3.parquet".to_string()));
        assert!(file_names.contains(&"file4.parquet".to_string()));
        
        Ok(())
    }

    #[test]
    fn test_find_parquet_files_empty_directory() -> Result<()> {
        let temp_dir = TempDir::new()?;
        
        let result = find_parquet_files(&temp_dir.path().to_path_buf(), false)?;
        assert_eq!(result.len(), 0);
        
        Ok(())
    }

    #[test]
    fn test_consolidate_parquet_files() -> Result<()> {
        let temp_dir = TempDir::new()?;
        
        // Create test input files
        let file1 = temp_dir.path().join("file1.parquet");
        let file2 = temp_dir.path().join("file2.parquet");
        let output_file = temp_dir.path().join("output.parquet");
        
        create_test_parquet_file(&file1, 0, 10)?;
        create_test_parquet_file(&file2, 10, 20)?;
        
        let input_files = vec![file1, file2];
        consolidate_parquet_files(&input_files, &output_file, false)?;
        
        // Verify output file exists
        assert!(output_file.exists());
        
        // Verify the consolidated file has the expected number of rows
        let df = LazyFrame::scan_parquet(&output_file, Default::default())?
            .collect()?;
        
        assert_eq!(df.height(), 20); // 10 + 10 rows
        
        Ok(())
    }

    #[test]
    fn test_consolidate_parquet_files_empty_input() {
        let temp_dir = TempDir::new().unwrap();
        let output_file = temp_dir.path().join("output.parquet");
        
        let result = consolidate_parquet_files(&[], &output_file, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No input files provided"));
    }

    #[test]
    fn test_consolidate_parquet_files_with_mismatched_schemas() -> Result<()> {
        let temp_dir = TempDir::new()?;
        
        // Create files with incompatible schemas
        let file1 = temp_dir.path().join("file1.parquet");
        let file2 = temp_dir.path().join("file2.parquet");
        let output_file = temp_dir.path().join("output.parquet");
        
        create_test_parquet_file(&file1, 0, 10)?;
        create_test_parquet_file_with_extra_column(&file2, 10, 20)?;
        
        let input_files = vec![file1, file2];
        let result = consolidate_parquet_files(&input_files, &output_file, false);
        
        // Schema mismatch should result in an error
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("concatenate"));
        
        Ok(())
    }
}
