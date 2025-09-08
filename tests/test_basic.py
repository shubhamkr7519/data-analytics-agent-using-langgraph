import pytest
import sys
from pathlib import Path

# Add src to path for testing
sys.path.append(str(Path(__file__).parent.parent))

from src.config import DATABASE_PATH
from src.data_processor import NYC311DataProcessor
from src.query_executor import QueryExecutor

def test_config_loading():
    """Test configuration loading"""
    assert DATABASE_PATH is not None
    assert isinstance(DATABASE_PATH, Path)

def test_data_processor_init():
    """Test data processor initialization"""
    processor = NYC311DataProcessor()
    assert processor.db_path == DATABASE_PATH

def test_query_executor_init():
    """Test query executor initialization"""
    executor = QueryExecutor()
    assert executor.db_path == DATABASE_PATH

def test_sql_safety():
    """Test SQL safety validation"""
    executor = QueryExecutor()
    
    # Safe query should pass
    safe_query = "SELECT complaint_type, COUNT(*) FROM nyc_311 GROUP BY complaint_type LIMIT 10"
    assert executor.validate_sql_safety(safe_query) == True
    
    # Unsafe query should fail
    with pytest.raises(ValueError):
        executor.validate_sql_safety("DROP TABLE nyc_311")
