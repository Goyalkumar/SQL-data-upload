"""
Unit Tests for bulk_upsert_enhanced.py

Run with: python -m pytest test_bulk_upsert.py -v
"""

import pytest
import pandas as pd
import os
from unittest.mock import Mock, patch, MagicMock
import sys

# Add parent directory to path to import the script
sys.path.insert(0, os.path.dirname(__file__))


class TestDataValidation:
    """Test data validation functions"""
    
    def test_valid_data(self):
        """Test validation passes for valid data"""
        from bulk_upsert_enhanced import validate_data, EXPECTED_COLUMNS
        
        df = pd.DataFrame({
            "Tag Sub Type ID": ["TAG001", "TAG002"],
            "Tag Sub Type": ["Type A", "Type B"],
            "Tag Type": ["Type1", "Type2"],
            "Tag Category": ["Cat1", "Cat2"],
            "SheetsName": ["Sheet1", "Sheet2"],
            "Tagging required?": ["Yes", "No"],
            "Type Code in TNP": ["CODE1", "CODE2"]
        })
        
        validated_df, errors = validate_data(df)
        
        assert len(errors) == 0, "Valid data should have no errors"
        assert len(validated_df) == 2
    
    def test_duplicate_ids(self):
        """Test duplicate ID detection"""
        from bulk_upsert_enhanced import validate_data
        
        df = pd.DataFrame({
            "Tag Sub Type ID": ["TAG001", "TAG001"],  # Duplicate
            "Tag Sub Type": ["Type A", "Type B"],
            "Tag Type": ["Type1", "Type2"],
            "Tag Category": ["Cat1", "Cat2"],
            "SheetsName": ["Sheet1", "Sheet2"],
            "Tagging required?": ["Yes", "No"],
            "Type Code in TNP": ["CODE1", "CODE2"]
        })
        
        validated_df, errors = validate_data(df)
        
        assert len(errors) > 0, "Should detect duplicate IDs"
        assert any("Duplicate" in err for err in errors)
    
    def test_null_required_fields(self):
        """Test NULL detection in required fields"""
        from bulk_upsert_enhanced import validate_data
        
        df = pd.DataFrame({
            "Tag Sub Type ID": ["TAG001", "TAG002"],
            "Tag Sub Type": [None, "Type B"],  # NULL in required field
            "Tag Type": ["Type1", "Type2"],
            "Tag Category": ["Cat1", "Cat2"],
            "SheetsName": ["Sheet1", "Sheet2"],
            "Tagging required?": ["Yes", "No"],
            "Type Code in TNP": ["CODE1", "CODE2"]
        })
        
        validated_df, errors = validate_data(df)
        
        assert len(errors) > 0, "Should detect NULL in required field"
        assert any("Tag Sub Type" in err and "NULL" in err for err in errors)
    
    def test_invalid_id_format(self):
        """Test invalid character detection in IDs"""
        from bulk_upsert_enhanced import validate_data
        
        df = pd.DataFrame({
            "Tag Sub Type ID": ["TAG 001", "TAG@002"],  # Invalid characters
            "Tag Sub Type": ["Type A", "Type B"],
            "Tag Type": ["Type1", "Type2"],
            "Tag Category": ["Cat1", "Cat2"],
            "SheetsName": ["Sheet1", "Sheet2"],
            "Tagging required?": ["Yes", "No"],
            "Type Code in TNP": ["CODE1", "CODE2"]
        })
        
        validated_df, errors = validate_data(df)
        
        assert len(errors) > 0, "Should detect invalid characters in ID"
        assert any("Invalid characters" in err for err in errors)


class TestCSVLoading:
    """Test CSV loading functionality"""
    
    def test_load_valid_csv(self, tmp_path):
        """Test loading a valid CSV file"""
        from bulk_upsert_enhanced import load_csv
        
        # Create temporary CSV
        csv_file = tmp_path / "test.csv"
        df_test = pd.DataFrame({
            "Tag Sub Type ID": ["TAG001"],
            "Tag Sub Type": ["Type A"],
            "Tag Type": ["Type1"],
            "Tag Category": ["Cat1"],
            "SheetsName": ["Sheet1"],
            "Tagging required?": ["Yes"],
            "Type Code in TNP": ["CODE1"]
        })
        df_test.to_csv(csv_file, index=False)
        
        # Load CSV
        df_loaded = load_csv(str(csv_file), validate=False)
        
        assert len(df_loaded) == 1
        assert df_loaded["Tag Sub Type ID"][0] == "TAG001"
    
    def test_missing_columns(self, tmp_path):
        """Test error on missing required columns"""
        from bulk_upsert_enhanced import load_csv
        
        # Create CSV with missing column
        csv_file = tmp_path / "test_missing.csv"
        df_test = pd.DataFrame({
            "Tag Sub Type ID": ["TAG001"],
            "Tag Sub Type": ["Type A"]
            # Missing other required columns
        })
        df_test.to_csv(csv_file, index=False)
        
        # Should raise ValueError
        with pytest.raises(ValueError, match="Missing required columns"):
            load_csv(str(csv_file), validate=False)
    
    def test_empty_csv(self, tmp_path):
        """Test error on empty CSV"""
        from bulk_upsert_enhanced import load_csv
        
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("")
        
        with pytest.raises(ValueError, match="empty"):
            load_csv(str(csv_file), validate=False)


class TestEnvironmentValidation:
    """Test environment validation"""
    
    @patch.dict(os.environ, {
        'DB_DRIVER': 'test_driver',
        'DB_SERVER': 'test_server',
        'DB_NAME': 'test_db',
        'DB_USER': 'test_user',
        'DB_PASSWORD': 'test_pass'
    })
    def test_valid_environment(self):
        """Test validation passes with all required vars"""
        from bulk_upsert_enhanced import validate_environment
        
        # Should not raise exception
        validate_environment()
    
    @patch.dict(os.environ, {
        'DB_DRIVER': 'test_driver',
        # Missing other required vars
    }, clear=True)
    def test_missing_env_vars(self):
        """Test error on missing environment variables"""
        from bulk_upsert_enhanced import validate_environment
        
        with pytest.raises(EnvironmentError, match="Missing required environment variables"):
            validate_environment()


class TestRetryLogic:
    """Test retry mechanism"""
    
    def test_retry_success_on_second_attempt(self):
        """Test successful retry after first failure"""
        from bulk_upsert_enhanced import execute_with_retry
        import pyodbc
        
        # Mock function that fails once then succeeds
        mock_func = Mock(side_effect=[
            pyodbc.OperationalError("Timeout"),
            "Success"
        ])
        
        result = execute_with_retry(mock_func, "test_operation", max_retries=3)
        
        assert result == "Success"
        assert mock_func.call_count == 2
    
    def test_retry_exhausted(self):
        """Test retry gives up after max attempts"""
        from bulk_upsert_enhanced import execute_with_retry
        import pyodbc
        
        # Mock function that always fails
        mock_func = Mock(side_effect=pyodbc.OperationalError("Always fails"))
        
        with pytest.raises(pyodbc.OperationalError):
            execute_with_retry(mock_func, "test_operation", max_retries=3)
        
        assert mock_func.call_count == 3


class TestFileValidation:
    """Test file validation"""
    
    def test_file_exists(self, tmp_path):
        """Test validation passes for existing file"""
        from bulk_upsert_enhanced import validate_file_exists
        
        test_file = tmp_path / "test.csv"
        test_file.write_text("test")
        
        # Should not raise exception
        validate_file_exists(str(test_file))
    
    def test_file_not_found(self):
        """Test error on non-existent file"""
        from bulk_upsert_enhanced import validate_file_exists
        
        with pytest.raises(FileNotFoundError):
            validate_file_exists("/nonexistent/file.csv")


# Fixtures
@pytest.fixture
def sample_valid_df():
    """Fixture providing a valid DataFrame"""
    return pd.DataFrame({
        "Tag Sub Type ID": ["TAG001", "TAG002", "TAG003"],
        "Tag Sub Type": ["Type A", "Type B", "Type C"],
        "Tag Type": ["Type1", "Type2", "Type3"],
        "Tag Category": ["Cat1", "Cat2", "Cat3"],
        "SheetsName": ["Sheet1", "Sheet2", "Sheet3"],
        "Tagging required?": ["Yes", "No", "Yes"],
        "Type Code in TNP": ["CODE1", "CODE2", "CODE3"]
    })


@pytest.fixture
def sample_invalid_df():
    """Fixture providing an invalid DataFrame with errors"""
    return pd.DataFrame({
        "Tag Sub Type ID": ["TAG001", "TAG001", "TAG@003"],  # Duplicate + invalid char
        "Tag Sub Type": ["Type A", None, "Type C"],  # NULL
        "Tag Type": ["Type1", "Type2", "Type3"],
        "Tag Category": ["Cat1", "Cat2", "Cat3"],
        "SheetsName": ["Sheet1", "Sheet2", "Sheet3"],
        "Tagging required?": ["Yes", "Maybe", "Yes"],  # Invalid value
        "Type Code in TNP": ["CODE1", "CODE2", "CODE3"]
    })


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
