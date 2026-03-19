import sys
import re

filepath = r'c:\Users\Admin\Documents\CHATBOT SNOWFLAKE\streamlit_app.py'
with open(filepath, 'r', encoding='utf-8') as f:
    text = f.read()

pattern = r'sql_prompt = f\"\"\"You are a SQL expert analyzing stock market data\..*?Generate the SQL now:\"\"\"'
replacement = '''sql_prompt = f\"\"\"You are a SQL expert analyzing stock market data.
Database: FINANCE_AI_DB.STOCK_DATA.PRICES
Columns: date (DATE), ticker (VARCHAR), open (FLOAT), high (FLOAT), low (FLOAT), close (FLOAT), volume (FLOAT), adj_close (FLOAT)
Available tickers in DB: {', '.join(db_tickers)}
IMPORTANT: The database contains HISTORICAL data from 1973 to Present. For recent/current prices that the database may lag behind on, the app uses a separate Live API constraint.
User question: {prompt}
CRITICAL RULES:
1. Use ONLY Snowflake SQL syntax. Use CURRENT_DATE() not CURDATE(). Use DATE_FROM_PARTS() not MAKEDATE().
2. Generate EXACTLY ONE SQL SELECT statement. No semicolons. No UNION. No multiple queries.
3. Do NOT output any markdown, backticks, or conversational text (no "Note:", no explanations).
4. The `ticker` column is a STRING, so compare with quotes (e.g., ticker = 'AAPL'). Use exact matches.
5. Price calculations MUST handle potential 0/NULL values securely. e.g., NO division by zero. Use NULLIF.
6. For 'today' or 'this year', get the MOST RECENT data available.
7. For multi-stock comparisons, use WHERE ticker IN ('TICK1','TICK2',...) — do NOT use separate queries.
8. For time-series queries (trends, performance, comparison over time): return raw daily rows with date, ticker, and close columns. Do NOT aggregate into a single row per ticker. Use ORDER BY date and LIMIT {dynamic_limit}.
   EXCEPTION: If the user explicitly asks for "month by month" or "monthly", you MUST use GROUP BY DATE_TRUNC('MONTH', date) as DATE and AVG(close) as AVG_CLOSE. Replace 'date' with 'DATE' in SELECT. Do NOT use LIMIT for month-by-month queries.
9. For non-time-series queries, LIMIT results to 50 rows.
Generate the SQL now:\"\"\"'''

new_text = re.sub(pattern, replacement, text, flags=re.DOTALL)
if new_text != text:
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_text)
    print('Replaced successfully.')
else:
    print('Failed to find exact match.')
