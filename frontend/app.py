# frontend/app.py
import streamlit as st
import asyncio
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.analytics_agent import NYC311AnalyticsAgent
from src.data_processor import NYC311DataProcessor
from src.config import DATABASE_PATH
from src.utils import setup_logging, logger

# Configure Streamlit page
st.set_page_config(
    page_title="NYC 311 Analytics Agent",
    page_icon="🗽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize logging
setup_logging()

class StreamlitApp:
    def __init__(self):
        self.agent = None
        self.data_processor = None
        
    def initialize_session_state(self):
        """Initialize Streamlit session state"""
        # Initialize messages
        if "messages" not in st.session_state:
            st.session_state.messages = [
                {
                    "role": "assistant",
                    "content": "Hello! I'm your NYC 311 Service Requests analytics agent. I can help you analyze complaint data with questions like:\n\n• What are the top 10 complaint types?\n• Which borough has the most complaints?\n• What percentage of complaints are closed within 3 days?\n• Which ZIP code has the highest number of complaints?\n\nWhat would you like to explore?",
                    "timestamp": datetime.now()
                }
            ]
        
        # Initialize flags with explicit checks
        if "agent_initialized" not in st.session_state:
            st.session_state.agent_initialized = False
            
        if "data_loaded" not in st.session_state:
            st.session_state.data_loaded = False
            
        # CRITICAL: Always ensure agent exists in session state
        if "agent" not in st.session_state:
            st.session_state.agent = None

    def render_sidebar(self):
        """Render sidebar with configuration and stats"""
        with st.sidebar:
            st.title("🗽 NYC 311 Analytics")
            st.markdown("---")
            
            # Agent status with explicit checks
            st.subheader("🤖 Agent Status")
            if (hasattr(st.session_state, 'agent_initialized') and 
                st.session_state.agent_initialized and 
                hasattr(st.session_state, 'agent') and 
                st.session_state.agent is not None):
                st.success("✅ Agent Ready")
            else:
                st.warning("⚠️ Agent Not Initialized")
            
            # Data status
            st.subheader("📊 Data Status")
            if (hasattr(st.session_state, 'data_loaded') and 
                st.session_state.data_loaded):
                st.success("✅ Data Loaded")
                self.show_data_stats()
            else:
                st.warning("⚠️ Data Not Loaded")
                
            st.markdown("---")
            
            # Quick actions
            st.subheader("🚀 Quick Actions")
            if st.button("🔄 Refresh Agent", key="refresh_agent"):
                self.initialize_agent()
                st.rerun()
                
            if st.button("📈 Show Data Stats", key="show_stats"):
                self.show_detailed_stats()
                
            # Sample queries - FIXED with full text
            st.subheader("💡 Sample Queries")
            sample_queries = [
                "What are the top 10 complaint types?",
                "Which borough has the most complaints?", 
                "What percent complaints are closed within 3 days?",
                "Which ZIP code has highest complaints?",
                "What's the average resolution time by agency?",
                "How many complaints have coordinates?"
            ]
            
            for i, query in enumerate(sample_queries):
                if st.button(query, key=f"sample_query_{i}"):
                    # Add to messages and trigger processing
                    st.session_state.messages.append({
                        "role": "user",
                        "content": query,
                        "timestamp": datetime.now()
                    })
                    st.session_state.pending_query = query
                    st.rerun()

    def show_data_stats(self):
        """Show basic data statistics in sidebar"""
        try:
            if (hasattr(st.session_state, 'data_processor') and 
                st.session_state.data_processor is not None):
                stats = st.session_state.data_processor.get_analytical_stats()
                
                st.metric("Total Records", f"{stats['total_records']:,}")
                st.metric("Closure Rate", f"{stats['closure_rate']:.1f}%")
                st.metric("Geocoding Rate", f"{stats['geocoding_rate']:.1f}%")
                
        except Exception as e:
            st.error(f"Error loading stats: {e}")

    def show_detailed_stats(self):
        """Show detailed statistics in main area"""
        try:
            if (hasattr(st.session_state, 'data_processor') and 
                st.session_state.data_processor is not None):
                stats = st.session_state.data_processor.get_analytical_stats()
                
                st.subheader("📊 Dataset Overview")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Records", f"{stats['total_records']:,}")
                with col2:
                    st.metric("Closure Rate", f"{stats['closure_rate']:.1f}%")
                with col3:
                    st.metric("Geocoding Rate", f"{stats['geocoding_rate']:.1f}%")
                with col4:
                    date_range = stats['date_range']
                    st.metric("Date Range", f"{date_range.iloc[0]['min_date'][:4]} - {date_range.iloc[0]['max_date'][:4]}")
                
        except Exception as e:
            st.error(f"Error showing detailed stats: {e}")

    def initialize_agent(self):
        """Initialize the analytics agent - FIXED with proper session state handling"""
        try:
            with st.spinner("🤖 Initializing Analytics Agent..."):
                # Create agent
                agent = NYC311AnalyticsAgent()
                
                # CRITICAL: Set both the session state values atomically
                st.session_state.agent = agent
                st.session_state.agent_initialized = True
                
                st.success("✅ Agent initialized successfully!")
                logger.info("Analytics agent initialized")
                
        except Exception as e:
            st.error(f"❌ Failed to initialize agent: {e}")
            logger.error(f"Agent initialization failed: {e}")
            # Ensure clean state on failure
            st.session_state.agent_initialized = False
            st.session_state.agent = None

    def check_data_setup(self):
        """Check if data is set up and ready"""
        try:
            if not DATABASE_PATH.exists():
                st.warning("⚠️ Database not found. Setting up data...")
                return False
                
            # Store data processor in session state
            if not hasattr(st.session_state, 'data_processor') or st.session_state.data_processor is None:
                st.session_state.data_processor = NYC311DataProcessor()
            
            stats = st.session_state.data_processor.get_analytical_stats()
            
            if stats['total_records'] > 0:
                st.session_state.data_loaded = True
                return True
            else:
                st.warning("⚠️ Database is empty. Please load data first.")
                return False
                
        except Exception as e:
            st.error(f"❌ Data setup check failed: {e}")
            return False

    def render_visualization(self, viz_data):
        """Render visualization from agent response"""
        try:
            if not viz_data or "data" not in viz_data:
                return
                
            df = pd.DataFrame(viz_data["data"])
            
            if len(df) == 0:
                return
                
            chart_type = viz_data.get("type", "bar")
            x_col = viz_data.get("x_column")
            y_col = viz_data.get("y_column")
            
            if not x_col or not y_col or x_col not in df.columns or y_col not in df.columns:
                return
                
            # Create appropriate chart
            if chart_type == "bar":
                fig = px.bar(df, x=x_col, y=y_col, title="Query Results Visualization")
                fig.update_xaxes(tickangle=45)
            elif chart_type == "pie":
                fig = px.pie(df, names=x_col, values=y_col, title="Query Results Visualization")
            else:
                fig = px.bar(df, x=x_col, y=y_col, title="Query Results Visualization")
                
            st.plotly_chart(fig, use_container_width=True)
            
        except Exception as e:
            st.error(f"Visualization error: {e}")

    def render_setup_page(self):
        """Render data setup page if data not ready"""
        st.title("🔧 Data Setup Required")
        st.markdown("---")
        
        st.info("📋 **Setup Instructions:**")
        st.markdown("""
        1. **Download the NYC 311 dataset** from Kaggle
        2. **Place the CSV file** in the `data/raw/` folder as: `311_Service_Requests_from_2010_to_Present.csv`
        3. **Run the data processor** to load data into the database
        4. **Refresh the page** to start using the analytics agent
        """)
        
        # Manual data processing trigger
        st.subheader("🚀 Process Data")
        
        if st.button("📊 Load Data from CSV", type="primary"):
            try:
                with st.spinner("Processing data... This may take a few minutes."):
                    processor = NYC311DataProcessor()
                    processor.setup_database()
                    
                    # Check if CSV exists
                    csv_path = processor.raw_data_path
                    if csv_path.exists():
                        success = processor.process_and_load_data()
                        if success:
                            st.success("✅ Data loaded successfully!")
                            st.balloons()
                            st.rerun()
                        else:
                            st.error("❌ Data processing failed.")
                    else:
                        st.error(f"❌ CSV file not found at: {csv_path}")
                        
            except Exception as e:
                st.error(f"❌ Setup failed: {e}")

    def render_chat_interface(self):
        """Render the main chat interface - COMPLETELY FIXED"""
        
        # Display chat messages from history
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.write(message["content"])
                
                # Show visualization if available
                if "visualization" in message and message["visualization"]:
                    self.render_visualization(message["visualization"])

        # Handle pending query from sidebar or regular chat input
        prompt = None
        if hasattr(st.session_state, 'pending_query'):
            prompt = st.session_state.pending_query
            del st.session_state.pending_query
        else:
            # Regular chat input
            prompt = st.chat_input("Ask me about NYC 311 data...")

        # Process the prompt if we have one
        if prompt:
            # CRITICAL: Check agent exists with explicit attribute checks
            if (not hasattr(st.session_state, 'agent') or 
                st.session_state.agent is None or
                not hasattr(st.session_state, 'agent_initialized') or 
                not st.session_state.agent_initialized):
                st.error("❌ Agent not properly initialized. Please click 'Refresh Agent' in the sidebar.")
                return
            
            # Display user message if not already shown
            user_msg_exists = any(
                msg.get("role") == "user" and msg.get("content") == prompt 
                for msg in st.session_state.messages[-2:]
            )
            
            if not user_msg_exists:
                st.session_state.messages.append({
                    "role": "user", 
                    "content": prompt,
                    "timestamp": datetime.now()
                })
                with st.chat_message("user"):
                    st.write(prompt)

            # Generate assistant response - SIMPLIFIED (no threads)
            with st.chat_message("assistant"):
                with st.spinner("🧠 Analyzing your question..."):
                    try:
                        # SIMPLIFIED: Direct async call without threading
                        # Create new event loop to avoid conflicts
                        import asyncio
                        
                        # Create and run async function
                        async def run_query():
                            return await st.session_state.agent.process_query(prompt)
                        
                        # Use asyncio.run in a try/except to handle event loop conflicts
                        try:
                            response = asyncio.run(run_query())
                        except RuntimeError as e:
                            if "cannot be called from a running event loop" in str(e):
                                # Fallback: create new loop in thread-safe way
                                loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(loop)
                                try:
                                    response = loop.run_until_complete(run_query())
                                finally:
                                    loop.close()
                            else:
                                raise e
                        
                        # Display response
                        st.write(response["response"])
                        
                        # Show visualization if available
                        if response.get("visualization_data") and len(response["visualization_data"]) > 0:
                            self.render_visualization(response["visualization_data"])
                        
                        # Add to chat history
                        assistant_message = {
                            "role": "assistant",
                            "content": response["response"],
                            "timestamp": datetime.now(),
                            "sql_query": response.get("sql_query", ""),
                            "raw_data": response.get("raw_results", [])
                        }
                        
                        if response.get("visualization_data"):
                            assistant_message["visualization"] = response["visualization_data"]
                            
                        st.session_state.messages.append(assistant_message)
                        
                    except Exception as e:
                        error_msg = f"❌ Sorry, I encountered an error: {str(e)}"
                        st.write(error_msg)
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": error_msg,
                            "timestamp": datetime.now()
                        })
                        logger.error(f"Query processing error: {e}")

    def run(self):
        """Main application runner"""
        
        # Initialize session state first
        self.initialize_session_state()
        
        # Check data setup
        data_ready = self.check_data_setup()
        
        if not data_ready:
            self.render_setup_page()
            return
            
        # Initialize agent if not already done - with explicit checks
        if (not hasattr(st.session_state, 'agent_initialized') or 
            not st.session_state.agent_initialized or
            not hasattr(st.session_state, 'agent') or 
            st.session_state.agent is None):
            self.initialize_agent()
            
        # Render main interface
        self.render_sidebar()
        
        # Main content area
        st.title("🗽 NYC 311 Analytics Agent")
        st.markdown("Ask me anything about NYC service requests data!")
        st.markdown("---")
        
        # Render chat interface
        self.render_chat_interface()

# Run the application
if __name__ == "__main__":
    app = StreamlitApp()
    app.run()
