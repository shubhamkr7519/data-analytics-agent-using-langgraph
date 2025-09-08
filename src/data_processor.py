import pandas as pd
import sqlite3
from pathlib import Path
from loguru import logger
from datetime import datetime
import numpy as np
from src.config import DATABASE_PATH, DATA_RAW, CHUNK_SIZE

class NYC311DataProcessor:
    def __init__(self):
        self.db_path = DATABASE_PATH
        self.raw_data_path = DATA_RAW / "311_Service_Requests_from_2010_to_Present.csv"
        
        # Strategically chosen columns for maximum analytical value
        self.analytical_columns = [
            'Unique Key',           # Primary identifier
            'Created Date',         # Temporal analysis
            'Closed Date',          # Resolution tracking  
            'Agency',              # Department analysis
            'Agency Name',         # Full department names
            'Complaint Type',      # Main category (HIGH VALUE)
            'Descriptor',          # Sub-category analysis
            'Location Type',       # Venue analysis
            'Incident Zip',        # Geographic analysis
            'Incident Address',    # Address-level insights
            'Borough',            # Regional analysis
            'Status',             # Completion tracking
            'Resolution Description', # Outcome analysis
            'Community Board',     # Administrative districts
            'Latitude',           # Geocoding analysis
            'Longitude'           # Spatial analysis
        ]
        
        # Pre-compute common analytical queries for performance
        self.analytical_metrics = {
            'days_to_close': 'Resolution time analysis',
            'is_closed': 'Completion status binary',
            'has_coordinates': 'Data quality - geocoding',
            'year_created': 'Annual trends',
            'month_created': 'Seasonal patterns',
            'day_of_week': 'Weekly patterns',
            'hour_created': 'Daily patterns',
            'zip_clean': 'Standardized ZIP codes',
            'response_category': 'Resolution type classification'
        }

    def process_and_load_data(self):
        """Process CSV and load into SQLite"""
        if not self.raw_data_path.exists():
            logger.error(f"Data file not found: {self.raw_data_path}")
            return False
            
        logger.info("Starting data processing...")
        
        # Process in chunks to handle memory efficiently
        chunk_count = 0
        try:
            for chunk in pd.read_csv(self.raw_data_path, chunksize=CHUNK_SIZE):
                # Clean and prepare data
                processed_chunk = self.clean_data_chunk(chunk)
                
                # Load to database
                conn = sqlite3.connect(self.db_path)
                processed_chunk.to_sql('nyc_311', conn, if_exists='append', index=False)
                conn.close()
                
                chunk_count += 1
                logger.info(f"Processed chunk {chunk_count}")
                
        except Exception as e:
            logger.error(f"Data processing failed: {e}")
            return False
            
        logger.info("Data processing complete!")
        return True


    def setup_database(self):
        """Create optimized schema for analytics"""
        conn = sqlite3.connect(self.db_path)
        
        # Main analytical table - ADD the missing resolution_speed and is_priority columns
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS nyc_311 (
            unique_key INTEGER PRIMARY KEY,
            created_date DATETIME NOT NULL,
            closed_date DATETIME,
            agency TEXT NOT NULL,
            agency_name TEXT,
            complaint_type TEXT NOT NULL,
            descriptor TEXT,
            location_type TEXT,
            incident_zip TEXT,
            incident_address TEXT,
            borough TEXT,
            status TEXT NOT NULL,
            resolution_description TEXT,
            community_board TEXT,
            latitude REAL,
            longitude REAL,
            
            -- Pre-computed analytical fields
            days_to_close INTEGER,
            is_closed BOOLEAN,
            has_coordinates BOOLEAN,
            year_created INTEGER,
            month_created INTEGER,
            day_of_week INTEGER,
            hour_created INTEGER,
            zip_clean TEXT,
            response_category TEXT,
            resolution_speed TEXT,
            is_priority BOOLEAN
        );
        """
    
    # ... rest stays the same

        
        conn.execute(create_table_sql)
        
        # Create indexes SEPARATELY (this is the correct way for SQLite)
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_complaint_type ON nyc_311(complaint_type);",
            "CREATE INDEX IF NOT EXISTS idx_borough ON nyc_311(borough);",
            "CREATE INDEX IF NOT EXISTS idx_zip ON nyc_311(zip_clean);",
            "CREATE INDEX IF NOT EXISTS idx_agency ON nyc_311(agency);",
            "CREATE INDEX IF NOT EXISTS idx_status ON nyc_311(status);",
            "CREATE INDEX IF NOT EXISTS idx_created_date ON nyc_311(created_date);",
            "CREATE INDEX IF NOT EXISTS idx_year_month ON nyc_311(year_created, month_created);",
            "CREATE INDEX IF NOT EXISTS idx_closure_time ON nyc_311(status, days_to_close);",
            "CREATE INDEX IF NOT EXISTS idx_geocoded ON nyc_311(has_coordinates);"
        ]
        
        for idx in indexes:
            conn.execute(idx)
        
        # Summary tables for common aggregations (performance boost)
        self.create_summary_tables(conn)
        
        conn.commit()
        conn.close()
        logger.info("Optimized analytics database created")


    def create_summary_tables(self, conn):
        """Pre-aggregate common queries for performance"""
        
        # Daily complaint summary
        daily_summary_sql = """
        CREATE TABLE IF NOT EXISTS daily_summary (
            date DATE PRIMARY KEY,
            total_complaints INTEGER,
            total_closed INTEGER,
            avg_days_to_close REAL,
            top_complaint_type TEXT
        );
        """
        
        # Borough-wise summary
        borough_summary_sql = """
        CREATE TABLE IF NOT EXISTS borough_summary (
            borough TEXT PRIMARY KEY,
            total_complaints INTEGER,
            closure_rate REAL,
            avg_resolution_time REAL,
            top_complaint_type TEXT
        );
        """
        
        # Agency performance summary
        agency_summary_sql = """
        CREATE TABLE IF NOT EXISTS agency_summary (
            agency TEXT PRIMARY KEY,
            total_complaints INTEGER,
            closure_rate REAL,
            avg_resolution_time REAL,
            most_common_complaint TEXT
        );
        """
        
        conn.execute(daily_summary_sql)
        conn.execute(borough_summary_sql) 
        conn.execute(agency_summary_sql)

    def clean_data_chunk(self, chunk):
        """Enhanced cleaning with analytical focus"""
        
        # Select strategic columns only
        available_cols = [col for col in self.analytical_columns if col in chunk.columns]
        df = chunk[available_cols].copy()
        
        # Standardize column names
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        # Enhanced data cleaning
        df = self.clean_temporal_data(df)
        df = self.clean_geographic_data(df) 
        df = self.clean_categorical_data(df)
        df = self.add_analytical_features(df)
        
        return df

    def clean_temporal_data(self, df):
        """Robust datetime handling"""
        # Convert dates with error handling and explicit format
        df['created_date'] = pd.to_datetime(df['created_date'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
        df['closed_date'] = pd.to_datetime(df['closed_date'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')
        
        # Calculate resolution metrics
        df['days_to_close'] = (df['closed_date'] - df['created_date']).dt.days
        df['is_closed'] = df['status'].str.upper().str.contains('CLOSED', na=False)
        
        # Extract time features for pattern analysis
        df['year_created'] = df['created_date'].dt.year
        df['month_created'] = df['created_date'].dt.month  
        df['day_of_week'] = df['created_date'].dt.dayofweek
        df['hour_created'] = df['created_date'].dt.hour
        
        return df


    def clean_geographic_data(self, df):
        """Geographic data standardization"""
        # Clean ZIP codes
        df['zip_clean'] = df['incident_zip'].fillna('').astype(str).str[:5]
        df['zip_clean'] = df['zip_clean'].replace('nan', '').replace('', 'UNKNOWN')
        
        # Standardize borough names
        df['borough'] = df['borough'].str.upper().str.strip()
        borough_mapping = {
            'NEW YORK': 'MANHATTAN',
            'KINGS': 'BROOKLYN', 
            'QUEENS': 'QUEENS',
            'BRONX': 'BRONX',
            'RICHMOND': 'STATEN ISLAND'
        }
        df['borough'] = df['borough'].map(borough_mapping).fillna(df['borough'])
        
        # Geocoding quality
        df['has_coordinates'] = (~df['latitude'].isna()) & (~df['longitude'].isna())
        
        return df

    def clean_categorical_data(self, df):
        """Categorical data optimization"""
        # Standardize complaint types
        df['complaint_type'] = df['complaint_type'].str.strip().str.title()
        
        # Clean agency names
        df['agency'] = df['agency'].str.strip().str.upper()
        
        # Categorize resolution patterns
        df['response_category'] = df['resolution_description'].apply(self.categorize_resolution)
        
        return df

    def categorize_resolution(self, resolution_text):
        """Intelligent resolution categorization"""
        if pd.isna(resolution_text):
            return 'NO_DESCRIPTION'
        
        text = str(resolution_text).lower()
        
        if any(word in text for word in ['summons', 'violation', 'issued', 'ticket']):
            return 'ENFORCEMENT_ACTION'
        elif any(word in text for word in ['fixed', 'repaired', 'completed', 'resolved']):
            return 'PROBLEM_RESOLVED'  
        elif any(word in text for word in ['no evidence', 'not found', 'unfounded']):
            return 'NO_VIOLATION_FOUND'
        elif any(word in text for word in ['duplicate', 'referred']):
            return 'ADMINISTRATIVE'
        else:
            return 'OTHER_ACTION'

    def add_analytical_features(self, df):
        """Add computed fields for common analytics"""
        # Resolution efficiency categories - convert to string
        df['resolution_speed'] = pd.cut(df['days_to_close'], 
                                    bins=[-1, 1, 3, 7, 30, float('inf')],
                                    labels=['SAME_DAY', 'WITHIN_3_DAYS', 'WITHIN_WEEK', 
                                            'WITHIN_MONTH', 'OVER_MONTH']).astype(str)
        
        # Priority inference (based on complaint type)
        priority_types = ['Emergency', 'Water', 'Heat', 'Gas', 'Electric']
        df['is_priority'] = df['complaint_type'].str.contains('|'.join(priority_types), 
                                                            case=False, na=False)
        
        return df


    def get_analytical_stats(self):
        """Comprehensive dataset analytics"""
        conn = sqlite3.connect(self.db_path)
        
        stats = {}
        
        # Basic volume metrics  
        stats['total_records'] = pd.read_sql("SELECT COUNT(*) as count FROM nyc_311", conn).iloc[0]['count']
        stats['date_range'] = pd.read_sql("SELECT MIN(created_date) as min_date, MAX(created_date) as max_date FROM nyc_311", conn)
        
        # Data quality metrics
        stats['closure_rate'] = pd.read_sql("SELECT AVG(CAST(is_closed as FLOAT)) * 100 as rate FROM nyc_311", conn).iloc[0]['rate']
        stats['geocoding_rate'] = pd.read_sql("SELECT AVG(CAST(has_coordinates as FLOAT)) * 100 as rate FROM nyc_311", conn).iloc[0]['rate']
        
        # Top categories for query preparation
        stats['top_complaints'] = pd.read_sql("SELECT complaint_type, COUNT(*) as count FROM nyc_311 GROUP BY complaint_type ORDER BY count DESC LIMIT 10", conn)
        stats['borough_distribution'] = pd.read_sql("SELECT borough, COUNT(*) as count FROM nyc_311 GROUP BY borough ORDER BY count DESC", conn)
        
        conn.close()
        return stats

    def create_performance_views(self):
        """Create materialized views for common queries"""
        conn = sqlite3.connect(self.db_path)
        
        # Top complaint types view
        conn.execute("""
        CREATE VIEW IF NOT EXISTS v_top_complaints AS
        SELECT complaint_type, COUNT(*) as complaint_count,
               AVG(days_to_close) as avg_resolution_days,
               SUM(CASE WHEN is_closed THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as closure_rate
        FROM nyc_311 
        GROUP BY complaint_type 
        ORDER BY complaint_count DESC;
        """)
        
        # Geographic hotspots view
        conn.execute("""
        CREATE VIEW IF NOT EXISTS v_geographic_summary AS  
        SELECT borough, zip_clean, COUNT(*) as complaint_count,
               complaint_type as top_complaint_type,
               AVG(days_to_close) as avg_resolution_time
        FROM nyc_311
        GROUP BY borough, zip_clean, complaint_type
        ORDER BY complaint_count DESC;
        """)
        
        conn.commit()
        conn.close()
