# Data Archival Script - Logging Documentation

## Overview
The improved archival script now has comprehensive logging across three database tables and local log files. This document explains what data is logged where and the improvements made.

## Database Tables and Their Purpose

### 1. `process_logs` Table
**Purpose**: Tracks the overall execution of the archival process

**Fields and Data Logged**:
- `id`: Auto-increment primary key
- `table_name`: Name of the table being archived (from config)
- `process_type`: Type of process (default: 'DATA_ARCHIVAL')
- `start_time`: When the process started
- `end_time`: When the process completed/failed
- `status`: Current status ('RUNNING', 'COMPLETED', 'FAILED', 'PARTIAL')
- `total_files`: Total number of files to process
- `processed_files`: Number of files successfully processed
- `current_file`: Currently processing file name
- `current_batch`: Current batch number being processed
- `replica_lag`: Maximum replica lag detected
- `hll_value`: Maximum HLL (History List Length) value detected
- `error_message`: Error details if process fails
- `created_at`: Record creation timestamp
- `updated_at`: Last update timestamp

**When Updated**:
- Created at script start with status 'RUNNING'
- `total_files` updated after S3 file discovery
- `processed_files` updated after each file completion
- `current_file` and `current_batch` updated during processing
- Final update with end status and health metrics

### 2. `file_processing_logs` Table
**Purpose**: Tracks individual file processing details

**Fields and Data Logged**:
- `id`: Auto-increment primary key
- `process_log_id`: Foreign key linking to process_logs
- `file_name`: Full S3 path/name of the file being processed
- `file_size`: File size in bytes
- `start_time`: When file processing started
- `end_time`: When file processing completed/failed
- `status`: File processing status ('PROCESSING', 'COMPLETED', 'FAILED')
- `total_batches`: Total number of batches in the file
- `processed_batches`: Number of batches successfully processed
- `error_message`: Error details if file processing fails
- `created_at`: Record creation timestamp
- `updated_at`: Last update timestamp

**When Updated**:
- Created when file processing starts
- Updated when file processing completes or fails
- Tracks batch counts for monitoring progress

### 3. `batch_processing_logs` Table
**Purpose**: Tracks individual batch processing within files

**Fields and Data Logged**:
- `id`: Auto-increment primary key
- `process_log_id`: Foreign key linking to process_logs
- `file_name`: File name for this batch
- `batch_number`: Sequential batch number within the file
- `min_id`: Minimum ID in the batch
- `max_id`: Maximum ID in the batch
- `record_count`: Number of records in the batch
- `processing_time_seconds`: Time taken to process this batch
- `replica_lag`: Replica lag at time of processing
- `hll_value`: HLL value at time of processing
- `sleep_time`: Sleep time applied after this batch
- `created_at`: Record creation timestamp

**When Updated**:
- Created after each batch is successfully processed
- Provides detailed performance metrics per batch

## Local Log Files

### 1. Main Log File (`DataProcessingAllDetails_DD_MM_YYYY.txt`)
**Contents**:
- Script start/end timestamps
- File processing status messages
- Error messages
- Health check results
- Sleep time calculations

### 2. Processed Files Log (`DataProcessedFilesDetails_DD_MM_YYYY.txt`)
**Contents**:
- List of successfully processed files
- Used for resume functionality to skip already processed files

## Key Improvements Made

### 1. Enhanced Error Handling
**Problem Fixed**: Script continued processing even when archival table didn't exist
**Solution**: 
- Added custom `ProcessingError` exception
- Improved `run_sql_queries()` function to raise exceptions on SQL errors
- Critical errors (like missing tables) now stop the entire process
- Proper error propagation through all function levels

### 2. Proper Status Logging
**Problem Fixed**: Script always showed 'RUNNING' status even after errors
**Solution**:
- Added proper status updates in all error scenarios
- Status properly set to 'FAILED' when critical errors occur
- Status set to 'PARTIAL' when some files succeed and some fail
- Status set to 'COMPLETED' only when all files process successfully

### 3. Complete File Processing Logging
**Problem Fixed**: `file_processing_logs` table was not being used
**Solution**:
- Added `create_file_processing_log()` function
- Added `update_file_processing_log()` function
- Each file now gets proper logging with start/end times, batch counts, and status
- File-level errors are properly logged

### 4. Accurate File Counts
**Problem Fixed**: `total_files` and `processed_files` not properly updated
**Solution**:
- Added `update_process_file_counts()` function
- `total_files` updated after S3 file discovery
- `processed_files` updated after each successful file processing
- Counts reflect actual processing status

### 5. Code Optimization
**Improvements Made**:
- Reduced from ~800 lines to ~500 lines (37% reduction)
- Consolidated duplicate functions
- Removed redundant file operations
- Improved function organization and reusability
- Added proper docstrings and comments
- Eliminated global variable dependencies where possible

## Error Handling Flow

1. **Critical Errors** (missing tables, connection failures):
   - Process immediately stops
   - Status set to 'FAILED'
   - Error logged in `process_logs.error_message`
   - Script exits with error code

2. **File-Level Errors** (file corruption, S3 access issues):
   - Current file marked as 'FAILED' in `file_processing_logs`
   - Processing continues with next file
   - Final status becomes 'PARTIAL' if other files succeed

3. **Batch-Level Errors** (data processing issues):
   - Current file marked as 'FAILED'
   - Error logged with batch details
   - Processing continues with next file

## Monitoring and Alerting

The improved logging structure allows for:

1. **Real-time Monitoring**:
   - Query `process_logs` for current status
   - Monitor `current_file` and `current_batch` for progress
   - Track `processed_files` vs `total_files` for completion percentage

2. **Performance Analysis**:
   - Use `batch_processing_logs` for performance metrics
   - Analyze processing times and health metrics
   - Identify bottlenecks and optimization opportunities

3. **Error Analysis**:
   - Detailed error messages in all tables
   - File-level and batch-level error tracking
   - Historical error patterns for troubleshooting

## Usage Example

```bash
python improved_archival_script.py config.ini
```

The script will now:
1. Properly handle all error scenarios
2. Log comprehensive details to database tables
3. Update status correctly based on processing results
4. Stop processing on critical errors like missing tables
5. Provide detailed progress tracking and error reporting