# src/config.py (Updated)
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"

# Create directories if they don't exist
DATA_RAW.mkdir(parents=True, exist_ok=True)
DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

# API Configuration
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")

if not DEEPSEEK_API_KEY:
    raise ValueError("DEEPSEEK_API_KEY environment variable is required")

# Database
DATABASE_PATH = PROJECT_ROOT / os.getenv("DATABASE_PATH", "data/processed/nyc_311.db")

# App settings
DEBUG = os.getenv("DEBUG", "True").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Data processing settings
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))

# Streamlit configuration
STREAMLIT_CONFIG = {
    "theme": {
        "primaryColor": "#FF6B6B",
        "backgroundColor": "#FFFFFF", 
        "secondaryBackgroundColor": "#F0F2F6",
        "textColor": "#262730"
    }
}
