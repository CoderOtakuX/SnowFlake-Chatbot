import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Page config   
st.set_page_config(
    page_title="Financial AI Analyst",
    page_icon="💰",
    layout="wide"
)

# Get Snowflake session
session = get_active_session()

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(120deg, #1f77b4, #ff7f0e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .insight-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<p class="main-header">💰 Financial AI Analyst</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Ask questions about stock performance, trends, and get AI-powered insights</p>', unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.header("📊 Dataset Info")
    
    try:
        stats = session.sql("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT ticker) as total_stocks,
                MIN(date) as earliest_date,
                MAX(date) as latest_date
            FROM FINANCE_AI_DB.STOCK_DATA.PRICES
        """).collect()[0]
        
        st.metric("Total Records", f"{stats['TOTAL_RECORDS']:,}")
        st.metric("Unique Stocks", f"{stats['TOTAL_STOCKS']:,}")
        st.metric("Date Range", f"{stats['EARLIEST_DATE'].year} - {stats['LATEST_DATE'].year}")
    except:
        st.info("Loading dataset stats...")
    
    st.divider()
    
    st.header("💡 Example Questions")
    
    example_questions = [
        "What was Apple's highest price in 2023?",
        "Compare Tesla and Microsoft performance in 2022",
        "Show me top 10 stocks by average volume",
        "What was Amazon's price trend from 2020-2023?",
        "Which stocks had the biggest gains in 2021?",
        "Analyze Google's volatility over the years",
        "What was IBM's performance during COVID?",
        "Show me banking stocks performance in 2019"
    ]
    
    for i, question in enumerate(example_questions):
        if st.button(question, key=f"example_{i}"):
            st.session_state.selected_question = question
    
    st.divider()
    
    if st.button("🗑️ Clear Chat History"):
        st.session_state.messages = []
        st.rerun()

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "sql" in message and message["sql"]:
            with st.expander("📝 View Generated SQL"):
                st.code(message["sql"], language="sql")
        if "data" in message and message["data"] is not None:
            st.dataframe(message["data"], use_container_width=True)
            
            # Auto-generate charts
            if len(message["data"]) > 1:
                numeric_cols = message["data"].select_dtypes(include=['float64', 'int64']).columns.tolist()
                if len(numeric_cols) > 0:
                    if 'YEAR' in message["data"].columns:
                        st.line_chart(message["data"].set_index('YEAR')[numeric_cols[0]])
                    elif 'TICKER' in message["data"].columns and len(message["data"]) <= 20:
                        st.bar_chart(message["data"].set_index('TICKER')[numeric_cols[0]])

# Handle pre-selected question
if "selected_question" in st.session_state:
    prompt = st.session_state.selected_question
    del st.session_state.selected_question
else:
    prompt = st.chat_input("Ask about stock performance, trends, or get investment insights...")

if prompt:
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Generate response
    with st.chat_message("assistant"):
        with st.spinner("🤔 Analyzing your question..."):
            try:
                # Step 1: Generate SQL query using AI
                sql_generation_prompt = f"""You are a SQL expert analyzing stock market data.

Database: FINANCE_AI_DB.STOCK_DATA.PRICES
Columns: date (DATE), ticker (VARCHAR), open (FLOAT), high (FLOAT), low (FLOAT), close (FLOAT), volume (FLOAT)

User question: {prompt}

Generate ONLY a valid SQL SELECT query to answer this question. Return just the SQL code with no explanation or markdown formatting.
Use proper WHERE clauses, aggregations, and ORDER BY when needed. Limit results to 20 rows unless specified otherwise."""

                sql_response = session.sql(f"""
                    SELECT SNOWFLAKE.CORTEX.COMPLETE(
                        'mistral-large',
                        $${sql_generation_prompt}$$
                    ) as sql_query
                """).collect()[0]['SQL_QUERY']
                
                # Clean the SQL (remove markdown code fences if present)
                sql_query = sql_response.replace("```sql", "").replace("```", "").strip()
                
                # Show the generated SQL
                with st.expander("📝 Generated SQL Query"):
                    st.code(sql_query, language="sql")
                
                # Step 2: Execute the query
                try:
                    result_df = session.sql(sql_query).to_pandas()
                    
                    if len(result_df) > 0:
                        # Show data
                        st.dataframe(result_df, use_container_width=True)
                        
                        # Generate charts
                        numeric_cols = result_df.select_dtypes(include=['float64', 'int64']).columns.tolist()
                        
                        if len(numeric_cols) > 0 and len(result_df) > 1:
                            if 'YEAR' in result_df.columns:
                                st.subheader("📈 Trend Over Time")
                                st.line_chart(result_df.set_index('YEAR')[numeric_cols[0]])
                            elif 'TICKER' in result_df.columns and len(result_df) <= 20:
                                st.subheader("📊 Comparison")
                                st.bar_chart(result_df.set_index('TICKER')[numeric_cols[0]])
                        
                        # Step 3: Generate AI insights
                        insight_prompt = f"""You are a financial analyst. Based on this data, provide a clear analysis.

User question: {prompt}

Data results (first 10 rows):
{result_df.head(10).to_string()}

Provide a 3-4 sentence analysis covering:
1. What the data shows
2. Key insights or patterns
3. Investment perspective or implications

Be specific and reference the actual numbers from the data."""

                        insights = session.sql(f"""
                            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                                'mistral-large',
                                $${insight_prompt}$$
                            ) as insight
                        """).collect()[0]['INSIGHT']
                        
                        # Display insights
                        st.markdown(f'<div class="insight-box">💡 <strong>AI Analysis:</strong><br>{insights}</div>', unsafe_allow_html=True)
                        
                        # Save to history
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": insights,
                            "sql": sql_query,
                            "data": result_df
                        })
                    else:
                        msg = "No data found for your question. Try rephrasing or asking about a different time period or stock."
                        st.warning(msg)
                        st.session_state.messages.append({"role": "assistant", "content": msg, "sql": sql_query})
                        
                except Exception as sql_error:
                    error_msg = f"Error executing query: {str(sql_error)}"
                    st.error(error_msg)
                    st.code(sql_query, language="sql")
                    
                    # Try to provide helpful response
                    fallback = f"I generated a SQL query but it had an error. Try rephrasing your question with specific stock tickers (like AAPL, MSFT) or time periods (like 2023, Q1 2022)."
                    st.info(fallback)
                    st.session_state.messages.append({"role": "assistant", "content": fallback, "sql": sql_query})
                    
            except Exception as e:
                error_msg = f"Error: {str(e)}"
                st.error(error_msg)
                
                # Fallback conversational response
                try:
                    fallback_prompt = f"""You are a helpful financial AI assistant. The user asked: "{prompt}"

Provide a brief, helpful response explaining what kind of financial data analysis you can help with, 
and ask them to rephrase their question with specific stocks or time periods."""

                    fallback_response = session.sql(f"""
                        SELECT SNOWFLAKE.CORTEX.COMPLETE(
                            'mistral-large',
                            $${fallback_prompt}$$
                        ) as response
                    """).collect()[0]['RESPONSE']
                    
                    st.info(fallback_response)
                    st.session_state.messages.append({"role": "assistant", "content": fallback_response})
                except:
                    st.session_state.messages.append({"role": "assistant", "content": error_msg})

# Footer
st.divider()
st.caption("🤖 Powered by Snowflake Cortex AI | 📊 Analyzing 29.7M stock records | 📅 Data: 1962-2023")