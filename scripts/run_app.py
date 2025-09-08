"""
Application runner script
"""
import subprocess
import sys
import os
from pathlib import Path

def check_requirements():
    """Check if all requirements are met"""
    
    # Check if .env exists
    if not Path(".env").exists():
        print("❌ .env file not found. Please create it first.")
        return False
    
    # Check API key
    from dotenv import load_dotenv
    load_dotenv()
    
    if not os.getenv("DEEPSEEK_API_KEY") or os.getenv("DEEPSEEK_API_KEY") == "your_deepseek_api_key_here":
        print("❌ Please set your DEEPSEEK_API_KEY in .env file")
        return False
    
    print("✅ Requirements check passed")
    return True

def run_streamlit():
    """Run the Streamlit application"""
    
    if not check_requirements():
        return
    
    print("🚀 Starting NYC 311 Analytics Agent...")
    
    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            "frontend/app.py", 
            "--server.port=8501"
        ], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running Streamlit: {e}")
    except KeyboardInterrupt:
        print("\n👋 Application stopped")

if __name__ == "__main__":
    run_streamlit()
