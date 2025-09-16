# NYC 311 Analytics Agent

## Overview

The NYC 311 Analytics Agent is an intelligent chatbot application built with Streamlit, LangChain, and LangGraph that allows users to analyze NYC 311 service request data through natural language queries. The application provides insights into complaint patterns, borough distributions, resolution times, and other key metrics from NYC's 311 system.

## Features

- **Natural Language Query Processing**: Ask questions in plain English about NYC 311 data
- **Smart Response Handling**: Different response styles based on query complexity
- **Interactive Visualizations**: Automatic chart generation for top-N and comparison queries  
- **SQL Transparency**: Shows the generated SQL query used for each analysis
- **Sample Query Interface**: Pre-built buttons for common analytical questions
- **Real-time Data Processing**: Processes large datasets efficiently with optimized database queries
- **Session Management**: Maintains conversation context across interactions

## Architecture

### Core Components

1. **Frontend (Streamlit)**: Interactive web interface with chat capabilities
2. **Analytics Agent (LangGraph)**: Multi-step workflow for query processing
3. **Data Processor**: Handles CSV ingestion and database setup
4. **Query Executor**: Safe SQL query execution with validation
5. **Visualization Engine**: Dynamic chart generation with Plotly

### Workflow Pipeline

1. **Query Parsing**: LLM analyzes user intent and extracts parameters
2. **SQL Generation**: Converts natural language to safe SQL queries
3. **Query Execution**: Executes validated queries against SQLite database
4. **Result Analysis**: LLM provides insights and patterns from results
5. **Response Formatting**: Tailors response complexity based on query type
6. **Visualization**: Generates charts for visual data representation

## Technology Stack

- **Frontend**: Streamlit 1.28+
- **Backend**: Python 3.8+
- **Database**: SQLite with analytical optimizations
- **LLM Framework**: LangChain + LangGraph
- **AI Model**: DeepSeek Chat (configurable)
- **Visualization**: Plotly Express
- **Data Processing**: Pandas + NumPy

## Installation

### Setup Instructions

1. **Clone Repository**
```bash
git clone https://github.com/shubhamkr7519/data-analytics-agent-using-langgraph.git
cd data-analytics-agent-using-langgraph
```

2. **Create Virtual Environment**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install Dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure Environment**
Create a ``.env`` file in the root directory:
```
DEEPSEEK_API_KEY=your_deepseek_api_key_here
DEEPSEEK_BASE_URL=https://api.deepseek.com
DATABASE_PATH=data/processed/nyc_311.db
DATA_RAW=data/raw
CHUNK_SIZE=10000
```

5. **Launch Application**
```bash
streamlit run frontend/app.py
```

## Usage Guide

### Query Examples

**Simple Queries** (Concise responses):
- "What are the top 10 complaint types?"
- "Which borough has the most complaints?"
- "How many complaints were registered in 2020?"

**Detailed Queries** (Full analysis with insights):
- "Give me detailed insights on complaint patterns"
- "Provide comprehensive analysis of resolution times"
- "Show me detailed trends in noise complaints"

**Specific Analytics**:
- "What percentage of complaints are closed within 3 days?"
- "Which ZIP code has the highest number of complaints?"
- "What's the average resolution time by agency?"

### Interface Components

**Main Chat Interface**:
- Type queries in natural language
- View responses with optional visualizations
- See SQL queries used for transparency

**Sidebar Controls**:
- Agent status indicator
- Data loading statistics
- Quick action buttons
- Sample query buttons

**Data Visualization**:
- Automatic chart generation for appropriate queries
- Bar charts for top-N analyses
- Pie charts for distribution queries
- Interactive Plotly visualizations

## Configuration

### Database Schema

The application creates an optimized analytical database with:

**Main Table** (``nyc_311``):
- ``unique_key``: Primary identifier
- ``created_date``, ``closed_date``: Temporal fields
- ``complaint_type``, ``agency``, ``borough``: Categorical fields
- ``days_to_close``, ``is_closed``: Calculated metrics
- ``has_coordinates``: Data quality indicator
- ``zip_clean``: Standardized ZIP codes

**Indexes** for performance:
- Complaint type, borough, agency
- Date ranges and closure metrics
- Geographic and quality indicators

### Logging

- Application logs stored in ``logs/`` directory
- Log levels: INFO, WARNING, ERROR
- Automatic log rotation and cleanup
- Query processing and performance tracking

## Development

### Project Structure

```
src/
├── analytics_agent.py    # LangGraph workflow
├── data_processor.py     # Data ingestion and cleaning
├── query_executor.py     # Safe SQL execution
├── config.py            # Configuration management
└── utils.py             # Logging and utilities

frontend/
└── app.py               # Streamlit interface

data/
├── raw/                 # Original CSV files
└── processed/           # SQLite database

logs/                    # Application logs
requirements.txt         # Python dependencies
.env                     # Environment configuration
.README                  # Documentation
```

### Key Classes

**NYC311AnalyticsAgent**: Main LangGraph workflow
- ``parse_user_query()``: Intent extraction
- ``generate_sql_query()``: SQL generation
- ``execute_query()``: Safe query execution
- ``analyze_results()``: Result interpretation
- ``format_final_response()``: Response formatting

**NYC311DataProcessor**: Data management
- ``setup_database()``: Schema creation
- ``process_and_load_data()``: CSV processing
- ``clean_data_chunk()``: Data cleaning
- ``get_analytical_stats()``: Summary statistics

**StreamlitApp**: Frontend interface
- ``render_chat_interface()``: Chat UI
- ``render_sidebar()``: Control panel
- ``render_visualization()``: Chart display
- ``initialize_agent()``: Agent setup

### Adding New Features

1. **New Query Types**: Extend ``parse_user_query()`` intent recognition
2. **Additional Visualizations**: Modify ``prepare_visualization()`` method
3. **Custom Analyses**: Add new workflow nodes to LangGraph
4. **UI Enhancements**: Update Streamlit components in frontend

## Troubleshooting

### Common Issues

**Data Loading Fails**:
- Verify CSV file path and format
- Check available disk space
- Review data processing logs

**Agent Not Responding**:
- Confirm API key configuration
- Check network connectivity
- Verify DeepSeek API status

**Visualization Errors**:
- Ensure data format compatibility
- Check Plotly version compatibility
- Review chart data structure

**Performance Issues**:
- Optimize database indexes
- Adjust chunk size in configuration
- Monitor memory usage during processing

### Log Analysis

Check ``logs/`` directory for detailed error information:
- ``app.log``: General application logs
- Error traces for debugging
- Performance metrics and query timing

### Database Maintenance

```sql
-- Check database size
SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size();

-- Verify data integrity
SELECT COUNT(*) FROM nyc_311;

-- Check index usage
.schema nyc_311
```

## API Documentation

### Query Processing Flow

1. **Input Validation**: Checks query format and content
2. **Intent Parsing**: Extracts query type and parameters
3. **SQL Generation**: Creates safe, optimized queries
4. **Execution**: Runs queries with validation
5. **Analysis**: Provides insights and context
6. **Response**: Formats results based on complexity

## Security

### SQL Injection Prevention
- Parameterized queries only
- Read-only database access
- Query pattern restrictions

### API Security
- Secure API key storage
- Environment-based configuration
- Request validation and sanitization

### Data Privacy
- Local data processing
- No external data transmission

---

**Built with ❤️ for NYC data analysis and insights**
