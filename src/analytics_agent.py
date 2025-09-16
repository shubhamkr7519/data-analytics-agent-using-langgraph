# src/analytics_agent.py
from langgraph.graph import StateGraph, START, END
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
import asyncio
from typing import Dict, Any, List, Optional, TypedDict
import json
import sqlite3
from pathlib import Path
from src.config import DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL
from src.query_executor import QueryExecutor
from src.utils import logger
import re

# Import fix for async issues
if hasattr(asyncio, 'WindowsProactorEventLoopPolicy'):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

class NYC311AnalyticsState(TypedDict):
    """State management for the analytics workflow"""
    user_query: str
    parsed_intent: Dict
    sql_query: str
    query_results: List
    analysis_summary: str
    visualization_data: Dict
    final_response: str
    error_message: str

class NYC311AnalyticsAgent:
    def __init__(self):
        # Initialize LLM
        self.llm = ChatOpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url=DEEPSEEK_BASE_URL,
            model="deepseek-chat",
            temperature=0.1
        )
        
        # Initialize query executor
        self.query_executor = QueryExecutor()
        
        # Build the workflow graph
        self.workflow = self.build_workflow()
        
    def build_workflow(self):
        """Create the LangGraph workflow"""
        
        # Create state graph with TypedDict
        workflow = StateGraph(NYC311AnalyticsState)
        
        # Add nodes
        workflow.add_node("parse_query", self.parse_user_query)
        workflow.add_node("generate_sql", self.generate_sql_query)
        workflow.add_node("execute_query", self.execute_query)
        workflow.add_node("analyze_results", self.analyze_results)
        workflow.add_node("prepare_visualization", self.prepare_visualization)
        workflow.add_node("format_response", self.format_final_response)
        workflow.add_node("handle_error", self.handle_error)
        
        # Define the workflow flow
        workflow.add_edge(START, "parse_query")
        workflow.add_edge("parse_query", "generate_sql")
        workflow.add_edge("generate_sql", "execute_query")
        workflow.add_edge("execute_query", "analyze_results")
        
        # Conditional edge for visualization
        workflow.add_conditional_edges(
            "analyze_results",
            self.should_create_visualization,
            {
                "visualize": "prepare_visualization",
                "skip_viz": "format_response"
            }
        )
        
        workflow.add_edge("prepare_visualization", "format_response")
        workflow.add_edge("format_response", END)
        workflow.add_edge("handle_error", END)
        
        return workflow.compile()

    async def parse_user_query(self, state: NYC311AnalyticsState) -> NYC311AnalyticsState:
        """Parse user intent and extract key parameters"""
        
        system_prompt = """
        You are an expert data analyst for NYC 311 service requests. Parse the user's question and determine:

        1. Is this query related to NYC 311 data? (true/false)
        2. Is this a greeting? (true/false) 
        3. Query complexity level: "simple", "detailed", or "unrelated"
        4. If related to NYC 311 data, extract:
        - Query Type: (top_n, time_analysis, geographic_analysis, comparison, data_quality, trend_analysis)
        - Entity: What they want to analyze (complaint_type, agency, borough, zip_code, etc.)
        - Metric: What they want to measure (count, percentage, average_time, etc.)
        - Filters: Any conditions (date_range, specific_values, etc.)
        - Limit: Number of results needed (if applicable)

        Available database columns:
        - complaint_type, agency, borough, zip_clean, status, created_date, closed_date
        - days_to_close, is_closed, has_coordinates, year_created, month_created
        - response_category, resolution_speed, is_priority

        IMPORTANT: Respond with valid JSON only. No markdown, no explanations.

        Example response: {"related_to_data": true, "is_greeting": false, "complexity": "simple", "query_type": "top_n", "entity": "complaint_type", "metric": "count", "limit": 5}
        """
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"User Question: {state['user_query']}")
        ]
        
        try:
            response = await self.llm.ainvoke(messages)
            response_text = response.content.strip()
            response_text = re.sub(r'```\w*\n?', '', response_text)  # Remove ``````python, etc.
            response_text = re.sub(r'```\n?', '', response_text)     # Remove remaining ```
            response_text = response_text.strip()
            
            # Remove any remaining markdown or extra text
            if response_text.startswith('{') and response_text.endswith('}'):
                # Already clean JSON
                pass
            else:
                # Try to extract JSON from text
                json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                if json_match:
                    response_text = json_match.group(0)
            
            # Handle empty responses
            if not response_text or not response_text.strip():
                logger.warning("Empty response from LLM for query parsing")
                state["parsed_intent"] = {"related_to_data": False, "is_greeting": False, "complexity": "unrelated"}
                return state
                
            # CRITICAL FIX: Use json.loads (not eval) to handle true/false/null properly
            try:
                parsed_intent = json.loads(response_text)
                state["parsed_intent"] = parsed_intent
                logger.info(f"Parsed intent: {parsed_intent}")
            except json.JSONDecodeError as json_error:
                logger.error(f"JSON decode error: {json_error}. Response was: {response_text}")
                # Try one more cleanup attempt
                try:
                    # Last resort: manual cleanup of common JSON issues
                    cleaned_text = response_text.replace("'", '"')  # Single to double quotes
                    parsed_intent = json.loads(cleaned_text)
                    state["parsed_intent"] = parsed_intent
                    logger.info(f"Parsed intent after cleanup: {parsed_intent}")
                except:
                    # Fallback to safe default
                    state["parsed_intent"] = {"related_to_data": False, "is_greeting": False, "complexity": "unrelated"}
                
        except Exception as e:
            logger.error(f"Error parsing query: {e}")
            state["error_message"] = f"Could not understand the question: {e}"
            
        return state

    async def format_final_response(self, state: NYC311AnalyticsState) -> NYC311AnalyticsState:
        """Format the final user-facing response - FIXED with smart formatting"""
        
        if state.get("error_message"):
            state["final_response"] = f"I encountered an issue: {state['error_message']}\n\nPlease try rephrasing your question or ask something like:\n- 'What are the top 10 complaint types?'\n- 'Which borough has the most complaints?'\n- 'What percentage of complaints are closed within 3 days?'"
            return state

        # Handle unrelated queries
        if not state.get("parsed_intent", {}).get("related_to_data", True):
            if state.get("parsed_intent", {}).get("is_greeting", False):
                state["final_response"] = "Hello! I'm your NYC 311 Service Requests analytics agent. Please ask me questions about NYC service request data, such as:\n\n• What are the top complaint types?\n• Which borough has the most complaints?\n• What's the average resolution time?\n• How many complaints are geocoded?"
                return state
            else:
                state["final_response"] = "I'm sorry, but your question appears to be unrelated to NYC 311 service request data. I can only help you analyze NYC service requests data. Please ask questions about complaint types, boroughs, agencies, resolution times, or other aspects of the NYC 311 dataset."
                return state

        # Determine response complexity
        complexity = state.get("parsed_intent", {}).get("complexity", "simple")
        
        if complexity == "detailed" or "insight" in state.get("user_query", "").lower() or "detailed" in state.get("user_query", "").lower():
            # Full detailed response with all sections
            response_parts = []
            
            if state.get("analysis_summary"):
                response_parts.append(state["analysis_summary"])
                
            if state.get("query_results"):
                response_parts.append(f"\n**Detailed Results:**")
                for i, result in enumerate(state["query_results"][:10]):
                    if isinstance(result, dict):
                        formatted_result = ", ".join([f"{k}: {v}" for k, v in result.items()])
                        response_parts.append(f"{i+1}. {formatted_result}")
                        
            response_parts.append(f"\n*Query used: `{state.get('sql_query', '')}`*")
            state["final_response"] = "\n".join(response_parts)
            
        else:
            # Simple, concise response
            if state.get("query_results"):
                # Extract key numbers for simple response
                results = state.get("query_results", [])
                if len(results) > 0:
                    if isinstance(results[0], dict):
                        # Format the top results concisely
                        simple_response = f"Here are the results:\n\n"
                        for i, result in enumerate(results[:5]):  # Show top 5 only
                            if isinstance(result, dict):
                                # Format nicely based on result type
                                items = list(result.items())
                                if len(items) >= 2:
                                    simple_response += f"{i+1}. {items[0][1]}: {items[1][1]:,}\n"
                                else:
                                    formatted_result = ", ".join([f"{k}: {v}" for k, v in result.items()])
                                    simple_response += f"{i+1}. {formatted_result}\n"
                                    
                        if len(results) > 5:
                            simple_response += f"\n...and {len(results)-5} more results."
                            
                        simple_response += f"\n\n*Query used: `{state.get('sql_query', '')}`*"
                        state["final_response"] = simple_response
                    else:
                        state["final_response"] = f"Found {len(results)} results.\n\n*Query used: `{state.get('sql_query', '')}`*"
                else:
                    state["final_response"] = f"No results found.\n\n*Query used: `{state.get('sql_query', '')}`*"
            else:
                state["final_response"] = f"No data available.\n\n*Query used: `{state.get('sql_query', '')}`*"
                
        return state


    async def generate_sql_query(self, state: NYC311AnalyticsState) -> NYC311AnalyticsState:
        """Generate SQL query based on parsed intent"""
        
        if state.get("error_message"):
            return state
            
        system_prompt = """
        You are an expert SQL generator for NYC 311 analytics database using SQLite.
        
        Database Schema:
        Table: nyc_311
        Key columns: unique_key, created_date, closed_date, complaint_type, agency, borough, 
                    zip_clean, status, days_to_close, is_closed, has_coordinates, 
                    year_created, month_created, response_category, resolution_speed
        
        IMPORTANT SQLite Date Filtering Rules:
        - For year filtering: Use strftime('%Y', created_date) = '2012' (not year_created = 2012)
        - For month filtering: Use strftime('%m', created_date) = '01'
        - For date range: Use created_date BETWEEN '2012-01-01' AND '2012-12-31'
        - Always use string comparisons with strftime: '2012' not 2012
        
        Generate SAFE, READ-ONLY SQL queries. Use proper aggregations, filtering, and ordering.
        Always include LIMIT clauses for large result sets.
        
        Common patterns:
        - Top N: SELECT column, COUNT(*) as count FROM nyc_311 GROUP BY column ORDER BY count DESC LIMIT N
        - Year analysis: Use strftime('%Y', created_date) = 'YYYY'
        - Geographic: Use borough, zip_clean columns
        - Data quality: Use has_coordinates, is_closed columns
        
        Return only the SQL query, no explanation.
        """
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Generate SQL for: {state['parsed_intent']}")
        ]
        
        try:
            response = await self.llm.ainvoke(messages)
            sql_query = response.content.strip()
            
            # Clean up the SQL
            sql_query = sql_query.replace('``````', '').strip()
            
            # CRITICAL FIX: Replace year_created with proper date filtering
            import re
            if 'year_created' in sql_query:
                # Replace year_created = YYYY with strftime('%Y', created_date) = 'YYYY'
                sql_query = re.sub(r'year_created\s*=\s*(\d{4})', 
                                r"strftime('%Y', created_date) = '\1'", sql_query)
            
            state["sql_query"] = sql_query
            logger.info(f"Generated SQL: {sql_query}")
            
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            state["error_message"] = f"Could not generate query: {e}"
            
        return state


    async def execute_query(self, state: NYC311AnalyticsState) -> NYC311AnalyticsState:
        """Execute the SQL query safely"""
        
        if state.get("error_message") or not state.get("sql_query"):
            return state
            
        try:
            results = self.query_executor.execute_safe_query(state["sql_query"])
            state["query_results"] = results
            logger.info(f"Query executed successfully, {len(results)} rows returned")
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            state["error_message"] = f"Query execution failed: {e}"
            
        return state

    async def analyze_results(self, state: NYC311AnalyticsState) -> NYC311AnalyticsState:
        """Analyze query results and generate insights"""
        
        if state.get("error_message") or not state.get("query_results"):
            return state
            
        system_prompt = """
        You are a data analyst providing insights on NYC 311 service request data.
        Analyze the query results and provide:
        1. Key findings and numbers
        2. Notable patterns or trends  
        3. Actionable insights
        4. Context about what the data means
        
        Be concise but insightful. Use specific numbers from the results.
        """
        
        results_text = f"Query Results:\n{json.dumps(state['query_results'][:10], indent=2)}"
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Original Question: {state['user_query']}\n\n{results_text}")
        ]
        
        try:
            response = await self.llm.ainvoke(messages)
            state["analysis_summary"] = response.content
            logger.info("Analysis completed")
            
        except Exception as e:
            logger.error(f"Error in analysis: {e}")
            state["error_message"] = f"Analysis failed: {e}"
            
        return state

    def should_create_visualization(self, state: NYC311AnalyticsState) -> str:
        """Determine if visualization should be created"""
        
        if state.get("error_message"):
            return "skip_viz"
            
        # Create visualization for top_n, comparison, and geographic queries
        viz_types = ["top_n", "comparison", "geographic_analysis", "time_analysis"]
        
        if (state.get("parsed_intent", {}).get("query_type") in viz_types and 
            len(state.get("query_results", [])) > 1):
            return "visualize"
        else:
            return "skip_viz"

    async def prepare_visualization(self, state: NYC311AnalyticsState) -> NYC311AnalyticsState:
        """Prepare data for visualization"""
        
        # Simple visualization data preparation
        if len(state.get("query_results", [])) > 0:
            # Extract column names and data
            if isinstance(state["query_results"][0], dict):
                columns = list(state["query_results"][0].keys())
                state["visualization_data"] = {
                    "type": "bar",
                    "data": state["query_results"][:20],
                    "x_column": columns[0] if len(columns) > 0 else None,
                    "y_column": columns[1] if len(columns) > 1 else None
                }
            
        return state

    async def format_final_response(self, state: NYC311AnalyticsState) -> NYC311AnalyticsState:
        """Format the final user-facing response"""
        
        if state.get("error_message"):
            state["final_response"] = f"I encountered an issue: {state['error_message']}\n\nPlease try rephrasing your question or ask something like:\n- 'What are the top 10 complaint types?'\n- 'Which borough has the most complaints?'\n- 'What percentage of complaints are closed within 3 days?'"
            return state
            
        # Combine analysis with raw results
        response_parts = []
        
        # Add analysis summary
        if state.get("analysis_summary"):
            response_parts.append(state["analysis_summary"])
            
        # Add key data points
        if state.get("query_results"):
            response_parts.append(f"\n**Detailed Results:**")
            
            # Format results nicely
            for i, result in enumerate(state["query_results"][:10]):
                if isinstance(result, dict):
                    formatted_result = ", ".join([f"{k}: {v}" for k, v in result.items()])
                    response_parts.append(f"{i+1}. {formatted_result}")
                    
        # Add SQL query for transparency (optional)
        response_parts.append(f"\n*Query used: `{state.get('sql_query', '')}`*")
        
        state["final_response"] = "\n".join(response_parts)
        return state

    async def handle_error(self, state: NYC311AnalyticsState) -> NYC311AnalyticsState:
        """Handle any errors in the workflow"""
        
        error_response = f"""
        I apologize, but I encountered an error processing your question: {state.get('error_message', 'Unknown error')}
        
        Here are some example questions I can help with:
        - "What are the top 10 complaint types by number of records?"
        - "Which ZIP code has the highest number of complaints?"
        - "What percent of complaints are closed within 3 days?"
        - "How many complaints have valid coordinates?"
        - "What's the average resolution time by borough?"
        """
        
        state["final_response"] = error_response
        return state

    async def process_query(self, user_query: str) -> Dict[str, Any]:
        """Main entry point for processing user queries"""
        
        # Initialize state as dictionary
        state = {
            "user_query": user_query,
            "parsed_intent": {},
            "sql_query": "",
            "query_results": [],
            "analysis_summary": "",
            "visualization_data": {},
            "final_response": "",
            "error_message": ""
        }
        
        try:
            # Run the workflow
            final_state = await self.workflow.ainvoke(state)
            
            return {
                "response": final_state["final_response"],
                "visualization_data": final_state["visualization_data"],
                "sql_query": final_state["sql_query"],
                "raw_results": final_state["query_results"]
            }
            
        except Exception as e:
            logger.error(f"Workflow execution error: {e}")
            return {
                "response": f"Sorry, I encountered an unexpected error: {e}",
                "visualization_data": {},
                "sql_query": "",
                "raw_results": []
            }