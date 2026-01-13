import os
import logging
import json
import time
import pandas as pd
import pyodbc
from dotenv import load_dotenv
from datetime import datetime
from typing import Optional, Tuple, List
from contextlib import contextmanager
import argparse

# -----------------------------------
# 1. Configuration
# -----------------------------------
CSV_PATH = r"[filepath].csv"

EXPECTED_COLUMNS = [
    "Tag Sub Type ID",
    "Tag Sub Type",
    "Tag Type",
    "Tag Category",
    "SheetsName",
    "Tagging required?",
    "Type Code in TNP"
]

NULLABLE_COLUMNS = [
    "Tagging required?",
    "Type Code in TNP"
]

REQUIRED_COLUMNS = [
    "Tag Sub Type ID",
    "Tag Sub Type",
    "Tag Type",
    "Tag Category",
    "SheetsName"
]

# Performance settings
BATCH_SIZE = 1000
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds

# -----------------------------------
# 2. Enhanced Logging Setup
# -----------------------------------
class StructuredLogger:
    """Structured logging for better monitoring and debugging"""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging with both file and console handlers"""
        log_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(log_format)
        
        # File handler with rotation
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f'bulk_upsert_{datetime.now().strftime("%Y%m%d")}.log')
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(log_format)
        
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
    
    def log_operation(self, operation: str, status: str, **kwargs):
        """Log structured operation data"""
        log_data = {
            'operation': operation,
            'status': status,
            'timestamp': datetime.now().isoformat(),
            **kwargs
        }
        
        if status == 'error':
            self.logger.error(json.dumps(log_data, indent=2))
        elif status == 'warning':
            self.logger.warning(json.dumps(log_data, indent=2))
        else:
            self.logger.info(json.dumps(log_data, indent=2))
    
    def info(self, message: str):
        self.logger.info(message)
    
    def error(self, message: str, exc_info=False):
        self.logger.error(message, exc_info=exc_info)
    
    def warning(self, message: str):
        self.logger.warning(message)
    
    def debug(self, message: str):
        self.logger.debug(message)

# Initialize logger
logger = StructuredLogger(__name__)

# -----------------------------------
# 3. Configuration Validation
# -----------------------------------
def validate_environment():
    """Validate required environment variables exist"""
    required_vars = ['DB_DRIVER', 'DB_SERVER', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing_vars)}\n"
            f"Please check your .env file"
        )
    
    logger.info("✓ Environment variables validated")

def validate_file_exists(path: str):
    """Validate CSV file exists and is readable"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV file not found: {path}")
    
    if not os.access(path, os.R_OK):
        raise PermissionError(f"CSV file is not readable: {path}")
    
    file_size_mb = os.path.getsize(path) / (1024 * 1024)
    logger.info(f"✓ CSV file found: {path} ({file_size_mb:.2f} MB)")

# -----------------------------------
# 4. Database Connection with Context Manager
# -----------------------------------
load_dotenv()

@contextmanager
def get_db_connection():
    """Context manager for database connections with automatic cleanup"""
    conn = None
    try:
        logger.info("Establishing database connection...")
        conn = pyodbc.connect(
            f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={os.getenv('DB_PASSWORD')}",
            autocommit=False
        )
        logger.info("✓ Database connection established")
        yield conn
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

def execute_with_retry(func, operation_name: str, max_retries: int = MAX_RETRIES):
    """Execute database operation with retry logic for transient failures"""
    for attempt in range(max_retries):
        try:
            result = func()
            logger.log_operation(
                operation_name,
                'success',
                attempt=attempt + 1
            )
            return result
            
        except pyodbc.OperationalError as e:
            if attempt == max_retries - 1:
                logger.log_operation(
                    operation_name,
                    'error',
                    attempt=attempt + 1,
                    error=str(e)
                )
                raise
            
            wait_time = RETRY_BACKOFF ** attempt
            logger.warning(
                f"Retry {attempt + 1}/{max_retries} for {operation_name} "
                f"after error: {e}. Waiting {wait_time}s..."
            )
            time.sleep(wait_time)

# -----------------------------------
# 5. Data Validation
# -----------------------------------
def validate_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Comprehensive data validation
    Returns: (cleaned_dataframe, list_of_errors)
    """
    errors = []
    original_count = len(df)
    
    logger.info(f"Validating {original_count} records...")
    
    # 1. Check for duplicate Tag Sub Type IDs
    duplicates = df[df.duplicated(subset=['Tag Sub Type ID'], keep=False)]
    if not duplicates.empty:
        dupe_ids = duplicates['Tag Sub Type ID'].unique().tolist()
        errors.append(f"Duplicate Tag Sub Type IDs found: {dupe_ids}")
        logger.warning(f"Found {len(dupe_ids)} duplicate IDs")
    
    # 2. Validate required fields are not null
    for col in REQUIRED_COLUMNS:
        null_mask = df[col].isna() | (df[col].astype(str).str.strip() == '')
        null_rows = df[null_mask]
        
        if not null_rows.empty:
            null_indices = null_rows.index.tolist()
            errors.append(
                f"Required column '{col}' has {len(null_rows)} NULL/empty values "
                f"at rows: {null_indices[:10]}{'...' if len(null_indices) > 10 else ''}"
            )
    
    # 3. Validate Tag Sub Type ID format (alphanumeric, dash, underscore only)
    invalid_ids = df[~df['Tag Sub Type ID'].astype(str).str.match(r'^[A-Za-z0-9_-]+$')]
    if not invalid_ids.empty:
        invalid_examples = invalid_ids['Tag Sub Type ID'].head(5).tolist()
        errors.append(
            f"Invalid characters in Tag Sub Type ID. "
            f"Examples: {invalid_examples}"
        )
    
    # 4. Check for unexpected data types or formats
    for col in df.columns:
        if col not in NULLABLE_COLUMNS:
            # Check for extremely long values (potential data corruption)
            long_values = df[df[col].astype(str).str.len() > 255]
            if not long_values.empty:
                errors.append(
                    f"Column '{col}' has {len(long_values)} values exceeding 255 characters"
                )
    
    # 5. Validate specific column values if applicable
    if 'Tagging required?' in df.columns:
        valid_values = ['Yes', 'No', 'Y', 'N', None, '']
        invalid_tagging = df[
            df['Tagging required?'].notna() & 
            ~df['Tagging required?'].astype(str).str.strip().isin(valid_values)
        ]
        if not invalid_tagging.empty:
            invalid_examples = invalid_tagging['Tagging required?'].unique().tolist()
            errors.append(
                f"'Tagging required?' has invalid values. "
                f"Expected: {valid_values}. Found: {invalid_examples}"
            )
    
    # Log validation summary
    if errors:
        logger.log_operation(
            'data_validation',
            'warning',
            total_records=original_count,
            error_count=len(errors),
            errors=errors
        )
    else:
        logger.log_operation(
            'data_validation',
            'success',
            total_records=original_count,
            message='All validations passed'
        )
    
    return df, errors

# -----------------------------------
# 6. Read & Process CSV
# -----------------------------------
def load_csv(path: str, validate: bool = True) -> pd.DataFrame:
    """Load and validate CSV file"""
    logger.info(f"Loading CSV file: {path}")
    
    try:
        # Read CSV with proper encoding
        df = pd.read_csv(path, encoding="utf-8-sig")
        logger.info(f"✓ Loaded {len(df)} records from CSV")
        
        # Check for expected columns
        missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        extra_cols = set(df.columns) - set(EXPECTED_COLUMNS)
        if extra_cols:
            logger.warning(f"Extra columns found (will be ignored): {extra_cols}")
        
        # Select only expected columns
        df = df[EXPECTED_COLUMNS].copy()
        
        # Replace empty strings/whitespace with NULL
        df = df.replace(r'^\s*$', None, regex=True)
        
        # Ensure nullable columns get proper NULLs
        df[NULLABLE_COLUMNS] = df[NULLABLE_COLUMNS].where(
            pd.notnull(df[NULLABLE_COLUMNS]), None
        )
        
        # Validate data if requested
        if validate:
            df, validation_errors = validate_data(df)
            
            if validation_errors:
                logger.warning(
                    f"Data validation found {len(validation_errors)} issue(s). "
                    f"Review logs for details."
                )
                # In production, you might want to decide whether to proceed or abort
                # For now, we'll log and continue
        
        return df
        
    except pd.errors.EmptyDataError:
        raise ValueError("CSV file is empty")
    except pd.errors.ParserError as e:
        raise ValueError(f"CSV parsing error: {e}")
    except Exception as e:
        logger.error(f"Failed to load CSV: {e}", exc_info=True)
        raise

# -----------------------------------
# 7. Create Staging Table
# -----------------------------------
def create_staging_table(cursor):
    """Create temporary staging table with proper indexing"""
    logger.info("Creating staging table...")
    
    def _create():
        cursor.execute("""
        IF OBJECT_ID('tempdb..#ClassSheetsMapping_Staging') IS NOT NULL
            DROP TABLE #ClassSheetsMapping_Staging;

        CREATE TABLE #ClassSheetsMapping_Staging (
            [Tag Sub Type ID] NVARCHAR(255) NOT NULL,
            [Tag Sub Type] NVARCHAR(255),
            [Tag Type] NVARCHAR(255),
            [Tag Category] NVARCHAR(255),
            [SheetsName] NVARCHAR(255),
            [Tagging required?] NVARCHAR(50),
            [Type Code in TNP] NVARCHAR(255)
        );

        CREATE UNIQUE CLUSTERED INDEX IX_Stage_TagSubTypeID
            ON #ClassSheetsMapping_Staging ([Tag Sub Type ID]);
        """)
        logger.info("✓ Staging table created successfully")
    
    execute_with_retry(_create, "create_staging_table")

# -----------------------------------
# 8. Bulk Insert with Batching
# -----------------------------------
def bulk_insert(cursor, df: pd.DataFrame):
    """Bulk insert data into staging table with batching for large datasets"""
    total_rows = len(df)
    logger.info(f"Starting bulk insert of {total_rows} records...")
    
    insert_sql = """
    INSERT INTO #ClassSheetsMapping_Staging (
        [Tag Sub Type ID],
        [Tag Sub Type],
        [Tag Type],
        [Tag Category],
        [SheetsName],
        [Tagging required?],
        [Type Code in TNP]
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    
    cursor.fast_executemany = True
    
    # Process in batches
    batches_processed = 0
    start_time = time.time()
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = df.iloc[i:i + BATCH_SIZE]
        
        def _insert_batch():
            cursor.executemany(insert_sql, batch.values.tolist())
            return len(batch)
        
        rows_inserted = execute_with_retry(
            _insert_batch,
            f"bulk_insert_batch_{batches_processed + 1}"
        )
        
        batches_processed += 1
        progress = min(i + BATCH_SIZE, total_rows)
        
        if batches_processed % 10 == 0 or progress == total_rows:
            elapsed = time.time() - start_time
            rate = progress / elapsed if elapsed > 0 else 0
            logger.info(
                f"Progress: {progress}/{total_rows} rows "
                f"({100 * progress / total_rows:.1f}%) - "
                f"{rate:.0f} rows/sec"
            )
    
    elapsed_time = time.time() - start_time
    logger.log_operation(
        'bulk_insert',
        'success',
        total_rows=total_rows,
        batches=batches_processed,
        elapsed_seconds=round(elapsed_time, 2),
        rows_per_second=round(total_rows / elapsed_time, 2)
    )

# -----------------------------------
# 9. MERGE (UPSERT) with Statistics
# -----------------------------------
def merge_data(cursor) -> dict:
    """
    Merge data from staging to target table
    Returns statistics about the operation
    """
    logger.info("Starting MERGE operation...")
    
    merge_sql = """
    MERGE dbo.ClassSheetsMapping AS target
    USING #ClassSheetsMapping_Staging AS source
        ON target.[Tag Sub Type ID] = source.[Tag Sub Type ID]

    WHEN MATCHED THEN
        UPDATE SET
            target.[Tag Sub Type] = source.[Tag Sub Type],
            target.[Tag Type] = source.[Tag Type],
            target.[Tag Category] = source.[Tag Category],
            target.[SheetsName] = source.[SheetsName],
            target.[Tagging required?] = source.[Tagging required?],
            target.[Type Code in TNP] = source.[Type Code in TNP]

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            [Tag Sub Type ID],
            [Tag Sub Type],
            [Tag Type],
            [Tag Category],
            [SheetsName],
            [Tagging required?],
            [Type Code in TNP]
        )
        VALUES (
            source.[Tag Sub Type ID],
            source.[Tag Sub Type],
            source.[Tag Type],
            source.[Tag Category],
            source.[SheetsName],
            source.[Tagging required?],
            source.[Type Code in TNP]
        )
    
    OUTPUT $action AS Action;
    """
    
    def _merge():
        start_time = time.time()
        cursor.execute(merge_sql)
        
        # Get operation statistics
        results = cursor.fetchall()
        stats = {
            'INSERT': sum(1 for r in results if r.Action == 'INSERT'),
            'UPDATE': sum(1 for r in results if r.Action == 'UPDATE'),
            'total': len(results),
            'elapsed_seconds': round(time.time() - start_time, 2)
        }
        
        return stats
    
    stats = execute_with_retry(_merge, "merge_data")
    
    logger.log_operation(
        'merge_operation',
        'success',
        **stats
    )
    
    logger.info(
        f"✓ MERGE completed: {stats['INSERT']} inserted, "
        f"{stats['UPDATE']} updated, {stats['total']} total rows affected"
    )
    
    return stats

# -----------------------------------
# 10. Main Orchestration with Enhanced Error Handling
# -----------------------------------
def main(csv_path: str = CSV_PATH, dry_run: bool = False):
    """
    Main execution function
    
    Args:
        csv_path: Path to CSV file
        dry_run: If True, perform all steps except final commit
    """
    execution_start = time.time()
    
    try:
        logger.info("=" * 80)
        logger.info("BULK UPSERT PROCESS STARTED")
        logger.info("=" * 80)
        
        # Step 1: Validate environment
        validate_environment()
        
        # Step 2: Validate file exists
        validate_file_exists(csv_path)
        
        # Step 3: Load and validate CSV
        df = load_csv(csv_path)
        
        if dry_run:
            logger.info("DRY RUN MODE: Changes will NOT be committed")
        
        # Step 4: Database operations
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            try:
                # Create staging table
                create_staging_table(cursor)
                
                # Bulk insert
                bulk_insert(cursor, df)
                
                # Merge data
                stats = merge_data(cursor)
                
                if dry_run:
                    logger.info("DRY RUN: Rolling back all changes...")
                    conn.rollback()
                    logger.info("✓ Rollback completed")
                else:
                    logger.info("Committing transaction...")
                    conn.commit()
                    logger.info("✓ Transaction committed successfully")
                
                # Final summary
                total_time = time.time() - execution_start
                logger.info("=" * 80)
                logger.info("PROCESS COMPLETED SUCCESSFULLY")
                logger.info("=" * 80)
                logger.log_operation(
                    'bulk_upsert_complete',
                    'success',
                    total_records_processed=len(df),
                    records_inserted=stats.get('INSERT', 0),
                    records_updated=stats.get('UPDATE', 0),
                    total_execution_time_seconds=round(total_time, 2),
                    dry_run=dry_run
                )
                
                return stats
                
            except Exception as e:
                logger.error("Error during database operations, rolling back...", exc_info=True)
                conn.rollback()
                raise
            finally:
                cursor.close()
        
    except Exception as e:
        logger.log_operation(
            'bulk_upsert_process',
            'error',
            error_type=type(e).__name__,
            error_message=str(e)
        )
        logger.error("=" * 80)
        logger.error("PROCESS FAILED")
        logger.error("=" * 80)
        raise
    
    finally:
        total_time = time.time() - execution_start
        logger.info(f"Total execution time: {total_time:.2f} seconds")

# -----------------------------------
# 11. CLI Interface
# -----------------------------------
def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Bulk UPSERT CSV data to SQL Server database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Normal execution
  python bulk_upsert_enhanced.py
  
  # Dry run (preview without committing)
  python bulk_upsert_enhanced.py --dry-run
  
  # Custom CSV path
  python bulk_upsert_enhanced.py --csv-path "C:\\custom\\path\\data.csv"
        """
    )
    
    parser.add_argument(
        '--csv-path',
        type=str,
        default=CSV_PATH,
        help=f'Path to CSV file (default: {CSV_PATH})'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without committing to database'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=BATCH_SIZE,
        help=f'Batch size for bulk insert (default: {BATCH_SIZE})'
    )
    
    return parser.parse_args()

# -----------------------------------
# 12. Entry Point
# -----------------------------------
if __name__ == "__main__":
    args = parse_arguments()
    
    # Update global batch size if specified
    if args.batch_size != BATCH_SIZE:
        BATCH_SIZE = args.batch_size
        logger.info(f"Using custom batch size: {BATCH_SIZE}")
    
    try:
        main(csv_path=args.csv_path, dry_run=args.dry_run)
        exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)
