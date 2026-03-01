# test_connection.py  — run this first to verify
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv("dot.env")
try:
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )
    print("✅ Connection successful!")
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT()")
    print(cur.fetchone())
except Exception as e:
    print(f"❌ Failed: {e}")