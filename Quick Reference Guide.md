# Quick Reference Guide - Bulk UPSERT Script

## 🚀 Quick Start (5 Minutes)

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure database connection**
   ```bash
   # Copy template
   cp .env.template .env
   
   # Edit .env with your credentials
   # DB_SERVER=your-server
   # DB_NAME=your-database
   # DB_USER=your-username
   # DB_PASSWORD=your-password
   ```

3. **Run the script**
   ```bash
   # Windows
   python bulk_upsert_enhanced.py
   
   # Or use the launcher
   run_bulk_upsert.bat
   
   # Linux/Mac
   python3 bulk_upsert_enhanced.py
   
   # Or use the launcher
   chmod +x run_bulk_upsert.sh
   ./run_bulk_upsert.sh
   ```

## 📋 Common Commands

### Basic Execution
```bash
# Normal run (commits to database)
python bulk_upsert_enhanced.py

# Dry run (preview without committing)
python bulk_upsert_enhanced.py --dry-run

# Custom CSV path
python bulk_upsert_enhanced.py --csv-path "C:\path\to\file.csv"

# Custom batch size
python bulk_upsert_enhanced.py --batch-size 5000

# Combined options
python bulk_upsert_enhanced.py --csv-path "data.csv" --batch-size 2000 --dry-run
```

### Viewing Help
```bash
python bulk_upsert_enhanced.py --help
```

### Running Tests
```bash
# Install pytest first
pip install pytest

# Run all tests
pytest test_bulk_upsert.py -v

# Run specific test
pytest test_bulk_upsert.py::TestDataValidation::test_valid_data -v
```

## 🔍 Monitoring & Logs

### Log File Locations
```
./logs/bulk_upsert_YYYYMMDD.log
```

### View Latest Log
```bash
# Linux/Mac
tail -f logs/bulk_upsert_$(date +%Y%m%d).log

# Windows PowerShell
Get-Content "logs\bulk_upsert_$(Get-Date -Format 'yyyyMMdd').log" -Tail 50 -Wait

# Windows Command Prompt
type logs\bulk_upsert_20260110.log
```

### Search Logs for Errors
```bash
# Linux/Mac
grep -i "error" logs/*.log

# Windows PowerShell
Select-String -Path "logs\*.log" -Pattern "error" -CaseSensitive:$false
```

## ⚡ Performance Tips

### Batch Size Recommendations
| Records | Batch Size | Expected Time |
|---------|------------|---------------|
| 1K      | 1000       | < 10 sec      |
| 10K     | 2000       | ~ 30 sec      |
| 100K    | 5000       | ~ 5 min       |
| 1M      | 10000      | ~ 30 min      |

### Speed Up Processing
```bash
# Use larger batches for big files
python bulk_upsert_enhanced.py --batch-size 10000

# Ensure SQL Server has proper indexes
# Primary key on [Tag Sub Type ID] (should already exist)
```

## 🛠️ Troubleshooting

### Common Error Messages & Solutions

#### 1. "Missing required environment variables"
```
Solution: Check your .env file exists and contains all required variables
- DB_DRIVER
- DB_SERVER
- DB_NAME
- DB_USER
- DB_PASSWORD
```

#### 2. "CSV file not found"
```
Solution: 
- Verify the file path is correct
- Use absolute paths or relative to script location
- Check file isn't locked by another program (Excel)
```

#### 3. "pyodbc.Error: Data source name not found"
```
Solution: Install ODBC driver
Windows: Download from Microsoft
Linux: sudo apt-get install unixodbc-dev msodbcsql17
Mac: brew install unixodbc
```

#### 4. "Login failed for user"
```
Solution:
- Verify username/password in .env
- Check user has necessary permissions on database
- For Windows Auth, use Trusted_Connection=yes instead
```

#### 5. "Permission denied" on CSV
```
Solution:
- Close file in Excel or other programs
- Check file permissions
- Run as administrator (if needed)
```

#### 6. "Duplicate Tag Sub Type IDs found"
```
Solution:
- Review and fix duplicates in CSV
- Script will log which IDs are duplicated
- Check data source for data quality issues
```

#### 7. "Connection timeout"
```
Solution:
- Check network connectivity
- Verify SQL Server is running
- Check firewall settings
- Script will automatically retry 3 times
```

## 📊 Understanding Output

### Success Output
```
BULK UPSERT PROCESS STARTED
✓ Environment variables validated
✓ CSV file found: file.csv (2.34 MB)
✓ Loaded 5000 records from CSV
Validating 5000 records...
Progress: 5000/5000 rows (100.0%) - 5847 rows/sec
✓ MERGE completed: 150 inserted, 4850 updated
PROCESS COMPLETED SUCCESSFULLY
Total execution time: 8.55 seconds
```

### What the numbers mean:
- **150 inserted**: New records added to database
- **4850 updated**: Existing records that were updated
- **5847 rows/sec**: Processing speed
- **8.55 seconds**: Total execution time

### Warning Output
```
WARNING - Data validation found 3 issue(s):
  - Duplicate Tag Sub Type IDs found: ['TAG001']
  - Required column 'Tag Sub Type' has NULL values at rows: [45]
  - Invalid characters in Tag Sub Type ID
```

**Action**: Review and fix the CSV file before running again

## 🔐 Security Best Practices

### Protect .env file
```bash
# Linux/Mac - restrict permissions
chmod 600 .env

# Windows - use file properties
Right-click .env → Properties → Security → Edit
Remove all users except your service account
```

### Never commit .env
```bash
# Ensure .gitignore contains
.env
logs/
*.log
__pycache__/
```

### Use least privilege
```sql
-- Grant minimum permissions needed
GRANT SELECT, INSERT, UPDATE ON dbo.ClassSheetsMapping TO [your_user];
```

## 📅 Scheduling

### Windows Task Scheduler
```xml
Create task:
- Program: C:\Python\python.exe
- Arguments: C:\path\to\bulk_upsert_enhanced.py
- Start in: C:\path\to\
- Schedule: Daily at 2:00 AM
- Run whether user is logged on or not
- Run with highest privileges (if needed)
```

### Linux Cron
```bash
# Edit crontab
crontab -e

# Add line (run daily at 2 AM)
0 2 * * * cd /path/to/script && /usr/bin/python3 bulk_upsert_enhanced.py >> /var/log/bulk_upsert_cron.log 2>&1
```

## 🧪 Testing Workflow

### Before Production Deployment
1. **Test with small dataset**
   ```bash
   # Create test CSV with 10-100 rows
   python bulk_upsert_enhanced.py --csv-path test_small.csv --dry-run
   ```

2. **Verify results**
   ```sql
   -- Check staging table (during dry-run)
   SELECT * FROM #ClassSheetsMapping_Staging;
   
   -- Check actual table
   SELECT * FROM dbo.ClassSheetsMapping 
   WHERE [Tag Sub Type ID] IN ('TEST001', 'TEST002');
   ```

3. **Test with production-size dataset**
   ```bash
   python bulk_upsert_enhanced.py --dry-run
   ```

4. **Final production run**
   ```bash
   python bulk_upsert_enhanced.py
   ```

## 📈 Monitoring Checklist

- [ ] Log files are being created in logs/ directory
- [ ] Processing time is acceptable (< 30 min for your dataset)
- [ ] No validation errors in logs
- [ ] Insert/Update counts match expectations
- [ ] Database table has correct row count
- [ ] No failed retries in logs

## 🆘 Emergency Procedures

### Rollback Changes
```sql
-- If you need to rollback (only works before commit)
-- The script does this automatically on error

-- If changes were committed, restore from backup
RESTORE DATABASE YourDB 
FROM DISK = 'C:\Backups\YourDB_Before_Upsert.bak'
WITH REPLACE;
```

### Stop Long-Running Process
```bash
# Windows - Ctrl+C in terminal
# Or kill process in Task Manager

# Linux/Mac - Ctrl+C in terminal
# Or: ps aux | grep bulk_upsert
#     kill -9 <PID>
```

### Clear Staging Table (if stuck)
```sql
-- Manually drop staging table if process crashes
DROP TABLE IF EXISTS #ClassSheetsMapping_Staging;
```

## 💡 Tips & Tricks

1. **Always test with --dry-run first**
   ```bash
   python bulk_upsert_enhanced.py --dry-run
   ```

2. **Monitor first few runs closely**
   - Watch logs in real-time
   - Verify record counts
   - Check processing time

3. **Keep historical logs**
   ```bash
   # Don't delete logs immediately
   # Keep for at least 30 days for troubleshooting
   ```

4. **Backup before large operations**
   ```sql
   BACKUP DATABASE YourDB 
   TO DISK = 'C:\Backups\YourDB_Before_Upsert.bak';
   ```

5. **Use proper CSV encoding**
   - Save as UTF-8 in Excel
   - File → Save As → CSV UTF-8

## 📞 Getting Help

1. Check logs first: `./logs/bulk_upsert_YYYYMMDD.log`
2. Run with `--dry-run` to preview changes
3. Review validation errors in console output
4. Check this quick reference guide
5. Review full README.md documentation

## 🔄 Version Upgrade Checklist

When updating the script:
- [ ] Backup current version
- [ ] Test new version with --dry-run
- [ ] Compare results with old version
- [ ] Update documentation if needed
- [ ] Deploy during low-traffic period

---

**Last Updated**: 2026-01-10  
**Script Version**: 2.0.0
