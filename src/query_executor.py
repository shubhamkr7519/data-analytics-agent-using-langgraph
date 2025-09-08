# src/query_executor.py
import sqlite3
import pandas as pd
from typing import List, Dict, Any
import re
from src.config import DATABASE_PATH
from src.utils import logger

class QueryExecutor:
    def __init__(self):
        self.db_path = DATABASE_PATH
        
        # SQL safety patterns - only allow SELECT queries
        self.allowed_patterns = [
            r'^SELECT\s+',
            r'\bFROM\s+nyc_311\b',
            r'\bGROUP\s+BY\b',
            r'\bORDER\s+BY\b',
            r'\bLIMIT\s+\d+\b',
            r'\bWHERE\b',
            r'\bHAVING\b'
        ]
        
        # Dangerous patterns to block
        self.blocked_patterns = [
            r'\bDROP\b', r'\bDELETE\b', r'\bINSERT\b', 
            r'\bUPDATE\b', r'\bALTER\b', r'\bCREATE\b'
        ]

    def validate_sql_safety(self, sql_query: str) -> bool:
        """Ensure SQL query is safe (read-only)"""
        
        sql_upper = sql_query.upper().strip()
        
        # Block dangerous operations
        for pattern in self.blocked_patterns:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                raise ValueError(f"Unsafe SQL operation detected: {pattern}")
                
        # Must start with SELECT
        if not sql_upper.startswith('SELECT'):
            raise ValueError("Only SELECT queries are allowed")
            
        return True

    def execute_safe_query(self, sql_query: str) -> List[Dict[str, Any]]:
        """Execute SQL query with safety validation"""
        
        try:
            # Validate query safety
            self.validate_sql_safety(sql_query)
            
            # Execute query
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable column access by name
            
            cursor = conn.execute(sql_query)
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            results = [dict(row) for row in rows]
            
            conn.close()
            
            logger.info(f"Query executed successfully: {len(results)} rows")
            return results
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise Exception(f"Database query error: {str(e)}")

    def get_schema_info(self) -> str:
        """Get database schema information for LLM context"""
        
        schema_info = """
        NYC 311 Database Schema:
        
        Table: nyc_311
        Columns:
        - unique_key (INTEGER): Unique identifier for each complaint
        - created_date (DATETIME): When complaint was created
        - closed_date (DATETIME): When complaint was resolved (if closed)
        - complaint_type (TEXT): Category of complaint (e.g., 'Noise', 'Illegal Parking')
        - agency (TEXT): Handling agency (e.g., 'NYPD', 'DOT')
        - borough (TEXT): NYC borough ('MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX', 'STATEN ISLAND')
        - zip_clean (TEXT): Standardized ZIP code
        - status (TEXT): Current status of complaint
        - days_to_close (INTEGER): Days taken to close complaint
        - is_closed (BOOLEAN): Whether complaint is closed (1/0)
        - has_coordinates (BOOLEAN): Whether lat/lon are available (1/0)
        - year_created (INTEGER): Year complaint was created
        - month_created (INTEGER): Month complaint was created (1-12)
        - response_category (TEXT): Type of resolution taken
        - resolution_speed (TEXT): Speed category ('SAME_DAY', 'WITHIN_3_DAYS', etc.)
        
        Common Query Patterns:
        - Top complaints: GROUP BY complaint_type ORDER BY COUNT(*) DESC
        - Geographic analysis: GROUP BY borough or zip_clean
        - Time analysis: Use days_to_close, created_date columns
        - Closure rates: AVG(is_closed) * 100
        """
        
        return schema_info
