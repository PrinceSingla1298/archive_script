# Enhanced Resume Functionality Guide

## Overview
The improved archival script now includes comprehensive batch-level resume functionality that can handle various termination scenarios and duplicate key errors gracefully.

## Key Improvements Made

### 1. ✅ **Fixed Error Logging in process_logs**
**Problem**: Error messages weren't being logged in the `process_logs` table
**Solution**: 
- Enhanced `update_process_log()` function with proper error handling
- Added validation to ensure error messages are actually written to database
- Added warning if database logging fails

### 2. ✅ **Batch-Level Resume Functionality**
**Problem**: Script restarted from batch 1 even if it failed at batch 10
**Solution**: 
- **Dual Tracking System**: Tracks progress in both database and log files
- **Smart Resume Logic**: Resumes from the highest completed batch from either source
- **Detailed Batch Logging**: Each batch status is logged with timestamps

### 3. ✅ **Duplicate Key Error Handling**
**Problem**: Script stopped completely when encountering duplicate keys in archival
**Solution**:
- Added `DuplicateKeyError` exception class
- Graceful handling of duplicate keys - logs error but continues processing
- Prevents data loss by continuing with deletion even if archival fails

### 4. ✅ **Enhanced Database Schema**
**Improvements**:
- Added `last_processed_batch` field to `file_processing_logs`
- Added unique constraint to `batch_processing_logs` to prevent duplicates
- Added `ON DUPLICATE KEY UPDATE` for batch logging

## How Resume Functionality Works

### **Batch Tracking Methods**

#### 1. **Database Tracking** (Primary)
- `batch_processing_logs` table stores completed batches
- `get_last_processed_batch()` queries for the highest batch number
- Most reliable source as it's transactional

#### 2. **Log File Tracking** (Backup)
- `DataProcessingAllDetails_DD_MM_YYYY.txt` contains batch-level entries
- Format: `[timestamp] FILE: filename | BATCH: number | STATUS: status`
- `get_last_batch_from_log_file()` parses log for last completed batch

#### 3. **Smart Resume Logic**
```python
last_batch_db = get_last_processed_batch(filename)
last_batch_log = get_last_batch_from_log_file(log_files['main'], filename)
start_batch = max(last_batch_db, last_batch_log)
```

### **Log File Format**
```
=== Script Starts at: 17_08_2025_14_30_45 ===
STARTING: file1.csv (Size: 1024000 bytes)
[2025-08-17 14:30:46] FILE: file1.csv | BATCH: 1 | STATUS: PROCESSING
[2025-08-17 14:30:47] FILE: file1.csv | BATCH: 1 | STATUS: COMPLETED
[2025-08-17 14:30:48] FILE: file1.csv | BATCH: 2 | STATUS: PROCESSING
DUPLICATE_KEY_ERROR: Batch 2 - MySQL Error: Duplicate entry...
[2025-08-17 14:30:49] FILE: file1.csv | BATCH: 2 | STATUS: COMPLETED_WITH_DUPLICATES
[2025-08-17 14:30:50] FILE: file1.csv | BATCH: 3 | STATUS: PROCESSING
ERROR: Error processing batch 3 in file file1.csv: Table 'rewards_archives' doesn't exist
[2025-08-17 14:30:51] FILE: file1.csv | BATCH: 3 | STATUS: FAILED
```

## Resume Scenarios Handled

### **Scenario 1: Script Killed/Terminated**
**What Happens**:
- Process stops abruptly at any point
- Database may have partial transactions
- Log file contains last successful batch

**Resume Behavior**:
1. Script reads both database and log file
2. Finds last completed batch (e.g., batch 9)
3. Resumes from batch 10
4. Skips batches 1-9 automatically

**Example Output**:
```
Resuming file data_2025-08-17.csv from batch 10
Skipping already processed batch 1
Skipping already processed batch 2
...
Skipping already processed batch 9
Processing batch 10/50 - IDs: 19801 to 22000
```

### **Scenario 2: Server Reboot**
**What Happens**:
- System restarts unexpectedly
- All processes terminated
- Database connections lost

**Resume Behavior**:
- Same as Scenario 1
- Script automatically detects last position
- Continues seamlessly from last successful batch

### **Scenario 3: Duplicate Key Error**
**What Happens**:
- Archival insert fails due to existing data
- Old script would stop completely

**New Behavior**:
1. Logs duplicate key error
2. Continues with deletion (data still gets archived)
3. Marks batch as `COMPLETED_WITH_DUPLICATES`
4. Continues to next batch
5. Process completes successfully

### **Scenario 4: Critical Database Error**
**What Happens**:
- Missing table or connection failure
- Script should stop to prevent data loss

**Behavior**:
1. Error properly logged in `process_logs.error_message`
2. Status set to 'FAILED'
3. Script stops immediately
4. Resume will start from last successful batch when issue is fixed

### **Scenario 5: File-Level vs Batch-Level Resume**

#### **File-Level Resume** (Original)
- Only tracked completed files
- If file failed at batch 10, entire file reprocessed from batch 1

#### **Batch-Level Resume** (New)
- Tracks individual batch completion
- If file failed at batch 10, resumes from batch 11
- Much more efficient for large files

## Monitoring and Troubleshooting

### **Check Resume Status**
```sql
-- Check overall process status
SELECT * FROM process_logs WHERE id = <process_id>;

-- Check file processing status
SELECT * FROM file_processing_logs WHERE process_log_id = <process_id>;

-- Check last processed batch for a file
SELECT MAX(batch_number) as last_batch 
FROM batch_processing_logs 
WHERE process_log_id = <process_id> AND file_name = 'your_file.csv';

-- Check for duplicate key errors
SELECT * FROM batch_processing_logs 
WHERE process_log_id = <process_id> 
ORDER BY created_at DESC;
```

### **Log File Analysis**
```bash
# Find last completed batch for a file
grep "FILE: your_file.csv.*STATUS: COMPLETED" DataProcessingAllDetails_17_08_2025.txt | tail -1

# Check for errors
grep "ERROR\|DUPLICATE_KEY_ERROR" DataProcessingAllDetails_17_08_2025.txt

# Monitor real-time progress
tail -f DataProcessingAllDetails_17_08_2025.txt
```

## Performance Benefits

### **Before (File-Level Resume)**
- File fails at batch 45/50
- Restart processes all 50 batches again
- Wastes ~90% of processing time

### **After (Batch-Level Resume)**
- File fails at batch 45/50
- Restart processes only batches 46-50
- Saves ~90% of processing time

### **Duplicate Key Handling**
- **Before**: Script stops completely, manual intervention needed
- **After**: Logs error, continues processing, completes successfully

## Usage Examples

### **Normal Run**
```bash
python improved_archival_script_v2.py config.ini
```

### **After Interruption**
```bash
# Script automatically detects and resumes
python improved_archival_script_v2.py config.ini

# Output will show:
# Resuming file data_2025-08-17.csv from batch 23
# Skipping already processed batch 1
# ...
# Processing batch 23/100 - IDs: 46001 to 48200
```

### **Check Status During Run**
```sql
-- Real-time monitoring
SELECT 
    p.status,
    p.current_file,
    p.current_batch,
    p.processed_files,
    p.total_files,
    f.processed_batches,
    f.total_batches
FROM process_logs p
LEFT JOIN file_processing_logs f ON f.process_log_id = p.id AND f.status = 'PROCESSING'
WHERE p.status = 'RUNNING';
```

## Best Practices

1. **Monitor Log Files**: Keep an eye on duplicate key errors
2. **Database Maintenance**: Ensure archival tables exist before running
3. **Disk Space**: Log files can grow large during long runs
4. **Regular Cleanup**: Archive old batch_processing_logs periodically
5. **Testing**: Test resume functionality in dev environment first

The enhanced script now provides robust, production-ready resume functionality that handles all common failure scenarios while maintaining data integrity.