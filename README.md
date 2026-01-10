# SQL-data-upload
This is regarding uploading of the data from .csv file to SQL database for maintaining data repository in SQL database as source of truth for future data validation.
# Bulk UPSERT Script - Production Documentation

## Overview
Production-ready Python script for bulk upserting CSV data to SQL Server using MERGE operations with comprehensive error handling, validation, retry logic, and monitoring.

## Features

### ✅ **Production-Ready Enhancements**
- ✓ Comprehensive data validation with detailed error reporting
- ✓ Retry logic for transient database failures
- ✓ Structured logging with daily log rotation
- ✓ Batch processing for large datasets
- ✓ Context managers for proper resource cleanup
- ✓ Transaction management with rollback on errors
- ✓ Performance monitoring and statistics
- ✓ Dry-run mode for testing
- ✓ Command-line interface with arguments
- ✓ Environment validation on startup
- ✓ Progress tracking for long operations

## Prerequisites

### System Requirements
- Python 3.8 or higher
- SQL Server 2016 or higher
- Windows/Linux (adjust paths accordingly)

### Python Dependencies
```bash
pip install pandas pyodbc python-dotenv
```

### ODBC Driver
Ensure you have SQL Server ODBC driver installed:
- Windows: Usually pre-installed
- Linux: Install `unixodbc` and `msodbcsql17`

## Setup

### 1. Environment Configuration

Create a `.env` file in the same directory as the script:

```env
# Database Connection Settings
DB_DRIVER=ODBC Driver 17 for SQL Server
DB_SERVER=your-server-name
DB_NAME=your-database-name
DB_USER=your-username
DB_PASSWORD=your-password
```

**Security Best Practices for `.env` file:**
```bash
# On Linux/Mac - restrict file permissions
chmod 600 .env

# On Windows - use file properties to restrict access
# Right-click .env → Properties → Security → Advanced
# Remove all users except your service account
```

### 2. CSV File Format

Your CSV file must contain these columns:
```
Tag Sub Type ID,Tag Sub Type,Tag Type,Tag Category,SheetsName,Tagging required?,Type Code in TNP
```

**Required Columns** (cannot be NULL/empty):
- Tag Sub Type ID
- Tag Sub Type
- Tag Type
- Tag Category
- SheetsName

**Optional Columns** (can be NULL):
- Tagging required?
- Type Code in TNP

### 3. Database Table

Ensure the target table exists:

```sql
CREATE TABLE dbo.ClassSheetsMapping (
    [Tag Sub Type ID] NVARCHAR(255) NOT NULL PRIMARY KEY,
    [Tag Sub Type] NVARCHAR(255),
    [Tag Type] NVARCHAR(255),
    [Tag Category] NVARCHAR(255),
    [SheetsName] NVARCHAR(255),
    [Tagging required?] NVARCHAR(50),
    [Type Code in TNP] NVARCHAR(255)
);
```

## Usage

### Basic Usage
```bash
# Run with default settings
python bulk_upsert_enhanced.py
```

### Dry Run (Test Mode)
```bash
# Preview changes without committing to database
python bulk_upsert_enhanced.py --dry-run
```

### Custom CSV Path
```bash
# Specify custom CSV file location
python bulk_upsert_enhanced.py --csv-path "C:\Data\my_file.csv"
```

### Custom Batch Size
```bash
# Process in larger batches (for better performance with large files)
python bulk_upsert_enhanced.py --batch-size 5000
```

### Combined Options
```bash
# Dry run with custom path and batch size
python bulk_upsert_enhanced.py --csv-path "C:\Data\test.csv" --batch-size 2000 --dry-run
```

## Output & Logging

### Console Output
Real-time progress updates:
```
2026-01-10 14:30:15 - INFO - BULK UPSERT PROCESS STARTED
2026-01-10 14:30:15 - INFO - ✓ Environment variables validated
2026-01-10 14:30:15 - INFO - ✓ CSV file found: C:\...\file.csv (2.34 MB)
2026-01-10 14:30:16 - INFO - ✓ Loaded 5000 records from CSV
2026-01-10 14:30:16 - INFO - Validating 5000 records...
2026-01-10 14:30:18 - INFO - Progress: 1000/5000 rows (20.0%) - 500 rows/sec
...
2026-01-10 14:30:25 - INFO - ✓ MERGE completed: 150 inserted, 4850 updated
2026-01-10 14:30:25 - INFO - PROCESS COMPLETED SUCCESSFULLY
```

### Log Files
Logs are automatically saved to `logs/` directory:
- **Location**: `./logs/bulk_upsert_YYYYMMDD.log`
- **Format**: Daily rotation (one file per day)
- **Content**: Detailed structured JSON logs for debugging

**Example log entry:**
```json
{
  "operation": "merge_operation",
  "status": "success",
  "timestamp": "2026-01-10T14:30:25.123456",
  "INSERT": 150,
  "UPDATE": 4850,
  "total": 5000,
  "elapsed_seconds": 8.45
}
```

## Error Handling

### Validation Errors
The script performs comprehensive validation:

1. **Duplicate IDs**: Identifies duplicate Tag Sub Type IDs
2. **Missing Required Fields**: Checks for NULL/empty values in required columns
3. **Invalid Characters**: Validates Tag Sub Type ID format (alphanumeric, dash, underscore)
4. **Data Type Issues**: Detects values exceeding length limits
5. **Invalid Values**: Validates specific column values (e.g., "Tagging required?" must be Yes/No/Y/N)

**Example output when validation fails:**
```
WARNING - Data validation found 3 issue(s):
  - Duplicate Tag Sub Type IDs found: ['TAG001', 'TAG002']
  - Required column 'Tag Sub Type' has 2 NULL/empty values at rows: [45, 67]
  - 'Tagging required?' has invalid values. Expected: ['Yes', 'No', 'Y', 'N']. Found: ['Maybe']
```

### Database Errors
Automatic retry for transient failures:
- **Retry Count**: 3 attempts
- **Backoff Strategy**: Exponential (2^attempt seconds)
- **Recoverable Errors**: Connection timeouts, deadlocks

### File Errors
```
FileNotFoundError: CSV file not found: C:\path\to\file.csv
PermissionError: CSV file is not readable: C:\path\to\file.csv
ValueError: CSV parsing error: ...
```

## Performance Optimization

### Batch Size Tuning
| Dataset Size | Recommended Batch Size | Expected Time |
|--------------|------------------------|---------------|
| < 10K rows   | 1,000 (default)        | < 1 minute    |
| 10K - 100K   | 2,000 - 5,000          | 1-5 minutes   |
| 100K - 1M    | 5,000 - 10,000         | 5-30 minutes  |
| > 1M rows    | 10,000+                | 30+ minutes   |

### Performance Metrics
The script tracks:
- Rows per second processing rate
- Batch processing time
- Total execution time
- Insert/Update breakdown

**Example:**
```
Progress: 50000/50000 rows (100.0%) - 5847 rows/sec
Total execution time: 8.55 seconds
```

## Troubleshooting

### Common Issues

#### 1. Environment Variables Not Found
```
EnvironmentError: Missing required environment variables: DB_PASSWORD
```
**Solution**: Check your `.env` file exists and contains all required variables

#### 2. ODBC Driver Not Found
```
pyodbc.Error: ('01000', "[01000] [unixODBC][Driver Manager]Can't open lib...")
```
**Solution**: Install correct ODBC driver for your OS

#### 3. Permission Denied on CSV
```
PermissionError: CSV file is not readable
```
**Solution**: Check file permissions or close the file in Excel

#### 4. Memory Issues with Large Files
```
MemoryError: Unable to allocate array
```
**Solution**: Reduce batch size or process file in chunks

#### 5. Connection Timeout
```
pyodbc.OperationalError: Connection timeout
```
**Solution**: Script automatically retries. Check network/firewall settings if it persists.

## Monitoring & Alerts

### Key Metrics to Monitor
1. **Execution Time**: Track processing time trends
2. **Error Rate**: Monitor validation errors and database failures
3. **Insert/Update Ratio**: Detect data quality issues
4. **File Size**: Monitor CSV file size over time

### Integration with Monitoring Tools
Parse JSON logs for integration:
```python
# Example: Parse logs for monitoring dashboard
import json

with open('logs/bulk_upsert_20260110.log') as f:
    for line in f:
        if '{' in line:
            log_data = json.loads(line.split(' - ')[-1])
            # Send to monitoring system
            if log_data.get('status') == 'error':
                send_alert(log_data)
```

## Scheduled Execution

### Windows Task Scheduler
```batch
@echo off
cd C:\path\to\script
python bulk_upsert_enhanced.py
if %ERRORLEVEL% NEQ 0 (
    echo Script failed with error level %ERRORLEVEL%
    exit /b %ERRORLEVEL%
)
```

### Linux Cron
```bash
# Run daily at 2 AM
0 2 * * * cd /path/to/script && /usr/bin/python3 bulk_upsert_enhanced.py >> /var/log/bulk_upsert_cron.log 2>&1
```

## Security Checklist

- [ ] `.env` file has restricted permissions (600 on Linux, limited users on Windows)
- [ ] `.env` is in `.gitignore`
- [ ] Database user has minimum required permissions (INSERT, UPDATE, SELECT on target table)
- [ ] Script runs under service account with limited privileges
- [ ] Log files are monitored and rotated regularly
- [ ] CSV source directory has restricted access

## Maintenance

### Log Rotation
Logs are created daily. Implement cleanup:
```bash
# Linux: Delete logs older than 30 days
find ./logs -name "*.log" -mtime +30 -delete

# Windows PowerShell
Get-ChildItem -Path ".\logs\*.log" | Where-Object {$_.LastWriteTime -lt (Get-Date).AddDays(-30)} | Remove-Item
```

### Database Maintenance
```sql
-- Monitor table size
SELECT 
    COUNT(*) as TotalRecords,
    SUM(DATALENGTH([Tag Sub Type ID])) / 1024 / 1024 as SizeMB
FROM dbo.ClassSheetsMapping;

-- Check for fragmentation
SELECT 
    index_id,
    avg_fragmentation_in_percent
FROM sys.dm_db_index_physical_stats(
    DB_ID(), 
    OBJECT_ID('dbo.ClassSheetsMapping'), 
    NULL, NULL, 'LIMITED'
);
```

## Version History

**v2.0.0** (Current - Enhanced Production Version)
- ✓ Added comprehensive data validation
- ✓ Implemented retry logic with exponential backoff
- ✓ Added structured logging with JSON output
- ✓ Batch processing for large datasets
- ✓ Context managers for resource safety
- ✓ Dry-run mode for testing
- ✓ CLI interface with arguments
- ✓ Performance monitoring
- ✓ Progress tracking

**v1.0.0** (Original)
- Basic bulk upsert functionality

## Support & Contact

For issues or questions:
1. Check log files in `./logs/` directory
2. Review validation errors in console output
3. Verify `.env` configuration
4. Contact database administrator for permission issues

## License
Internal use only - [Your Company Name]
