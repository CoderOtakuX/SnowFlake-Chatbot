import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import snowflake.connector
from datetime import date, timedelta, datetime
import requests
import json
import os
try:
    import yfinance as yf
    yfinance_available = True
except ImportError:
    yfinance_available = False

# ─── PAGE CONFIG ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FinSight AI",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ─── THEME TOGGLE STATE ──────────────────────────────────────────────────────
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = True

# ─── THEME VARIABLES ─────────────────────────────────────────────────────────
def get_theme():
    if st.session_state.dark_mode:
        return {
            "bg_primary": "#060B18",
            "bg_secondary": "#0D1526",
            "bg_card": "#111D35",
            "bg_card2": "#0A1628",
            "border": "#1E3A5F",
            "border_gold": "#C9A84C",
            "text_primary": "#E8EDF5",
            "text_secondary": "#8A9BBE",
            "text_muted": "#4A607F",
            "gold": "#C9A84C",
            "gold_light": "#F0C96B",
            "gold_dim": "#7A6030",
            "accent_blue": "#1A6EBD",
            "green": "#2ECC71",
            "red": "#E74C3C",
            "plotly_paper": "#060B18",
            "plotly_plot": "#0D1526",
            "plotly_grid": "#1E3A5F",
            "plotly_text": "#8A9BBE",
        }
    else:
        return {
            "bg_primary": "#EAECF0",
            "bg_secondary": "#F0F2F5",
            "bg_card": "#F5F6F8",
            "bg_card2": "#E4E6EB",
            "border": "#C8CDD8",
            "border_gold": "#B8922A",
            "text_primary": "#0D1A2D",
            "text_secondary": "#3A4A63",
            "text_muted": "#6B7A99",
            "gold": "#9A7A20",
            "gold_light": "#B8922A",
            "gold_dim": "#D4C090",
            "accent_blue": "#1A6EBD",
            "green": "#27AE60",
            "red": "#C0392B",
            "plotly_paper": "#EAECF0",
            "plotly_plot": "#F0F2F5",
            "plotly_grid": "#C8CDD8",
            "plotly_text": "#3A4A63",
        }

T = get_theme()

# ─── GLOBAL CSS ──────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700&family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Reset & Base ── */
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"] {{
    background-color: {T['bg_primary']} !important;
    font-family: 'DM Sans', sans-serif;
    color: {T['text_primary']};
}}

[data-testid="stMain"] {{
    background-color: {T['bg_primary']} !important;
}}

/* Hide default Streamlit elements */
#MainMenu, footer, [data-testid="stToolbar"], [data-testid="stDecoration"],
[data-testid="collapsedControl"], header {{ display: none !important; }}

/* Scrollbar */
::-webkit-scrollbar {{ width: 5px; height: 5px; }}
::-webkit-scrollbar-track {{ background: {T['bg_primary']}; }}
::-webkit-scrollbar-thumb {{ background: {T['gold_dim']}; border-radius: 3px; }}

/* ── Top Nav ── */
.top-nav {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 2rem;
    height: 64px;
    background: {T['bg_card2']};
    border-bottom: 1px solid {T['border']};
    position: sticky;
    top: 0;
    z-index: 999;
    backdrop-filter: blur(12px);
}}

.nav-logo {{
    display: flex;
    align-items: center;
    gap: 10px;
}}

.nav-logo-text {{
    font-family: 'Playfair Display', serif;
    font-size: 1.4rem;
    font-weight: 700;
    color: {T['gold']};
    letter-spacing: 0.02em;
}}

.nav-logo-tag {{
    font-size: 0.6rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    color: {T['text_muted']};
    text-transform: uppercase;
    margin-top: 2px;
}}

.nav-pill {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 12px;
    border-radius: 20px;
    background: {'rgba(201,168,76,0.12)' if st.session_state.dark_mode else 'rgba(201,168,76,0.1)'};
    border: 1px solid {T['gold_dim']};
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    color: {T['gold']};
    text-transform: uppercase;
}}

.nav-dot {{ width: 6px; height: 6px; border-radius: 50%; background: {T['green']}; 
    animation: pulse 2s infinite; }}

@keyframes pulse {{
    0%, 100% {{ opacity: 1; transform: scale(1); }}
    50% {{ opacity: 0.5; transform: scale(0.8); }}
}}

/* ── Metrics Row ── */
.metrics-row {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 1px;
    background: {T['border']};
    border: 1px solid {T['border']};
    border-radius: 8px;
    overflow: hidden;
    margin-bottom: 1.5rem;
}}

.metric-cell {{
    background: {T['bg_card']};
    padding: 1rem 1.5rem;
    display: flex;
    flex-direction: column;
    gap: 4px;
}}

.metric-label {{
    font-size: 0.68rem;
    font-weight: 600;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: {T['text_muted']};
}}

.metric-value {{
    font-family: 'Playfair Display', serif;
    font-size: 1.6rem;
    font-weight: 700;
    color: {T['gold']};
    line-height: 1;
}}

.metric-sub {{
    font-size: 0.7rem;
    color: {T['text_muted']};
    font-family: 'JetBrains Mono', monospace;
}}

/* ── Section Header ── */
.section-header {{
    display: flex;
    align-items: center;
    gap: 12px;
    margin: 2rem 0 1rem;
    padding-bottom: 0.75rem;
    border-bottom: 1px solid {T['border']};
}}

.section-title {{
    font-family: 'Playfair Display', serif;
    font-size: 1.1rem;
    font-weight: 600;
    color: {T['text_primary']};
    letter-spacing: 0.01em;
}}

.section-line {{
    flex: 1;
    height: 1px;
    background: linear-gradient(to right, {T['border']}, transparent);
}}

.gold-tag {{
    font-size: 0.62rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: {T['gold']};
    background: {'rgba(201,168,76,0.1)' if st.session_state.dark_mode else 'rgba(184,146,42,0.1)'};
    padding: 3px 8px;
    border-radius: 3px;
    border: 1px solid {T['gold_dim']};
}}

/* ── Chat Messages ── */
.stChatMessage {{
    background: transparent !important;
}}

[data-testid="stChatMessage"] {{
    background: {T['bg_card']} !important;
    border: 1px solid {T['border']} !important;
    border-radius: 8px !important;
    margin-bottom: 0.75rem !important;
    padding: 1rem !important;
}}

/* ── Chat Input ── */
[data-testid="stChatInput"] {{
    background: {T['bg_card']} !important;
    border: 1px solid {T['border_gold']} !important;
    border-radius: 8px !important;
}}

[data-testid="stChatInputTextArea"] {{
    background: transparent !important;
    color: {T['text_primary']} !important;
    font-family: 'DM Sans', sans-serif !important;
}}

/* ── Buttons ── */
.stButton > button {{
    background: transparent !important;
    border: 1px solid {T['border']} !important;
    color: {T['text_secondary']} !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.78rem !important;
    font-weight: 500 !important;
    border-radius: 6px !important;
    padding: 0.4rem 0.9rem !important;
    transition: all 0.2s ease !important;
    letter-spacing: 0.02em !important;
    width: 100% !important;
    text-align: left !important;
}}

.stButton > button:hover {{
    border-color: {T['gold']} !important;
    color: {T['gold']} !important;
    background: {'rgba(201,168,76,0.06)' if st.session_state.dark_mode else 'rgba(184,146,42,0.06)'} !important;
}}

/* ── Download Button ── */
.stDownloadButton > button {{
    background: linear-gradient(135deg, {T['gold']}, {T['gold_light']}) !important;
    border: none !important;
    color: #060B18 !important;
    font-weight: 600 !important;
    font-size: 0.78rem !important;
    letter-spacing: 0.05em !important;
    border-radius: 6px !important;
    padding: 0.5rem 1.2rem !important;
    width: auto !important;
}}

/* ── Expander ── */
[data-testid="stExpander"] {{
    background: {T['bg_card2']} !important;
    border: 1px solid {T['border']} !important;
    border-radius: 6px !important;
}}

/* ── Select / Multiselect ── */
[data-testid="stMultiSelect"] > div,
[data-testid="stSelectbox"] > div {{
    background: {T['bg_card']} !important;
}}

.stMultiSelect [data-baseweb="select"] {{
    background: {T['bg_card']} !important;
    border-color: {T['border']} !important;
}}

/* ── Date Input ── */
[data-testid="stDateInput"] input {{
    background: {T['bg_card']} !important;
    border-color: {T['border']} !important;
    color: {T['text_primary']} !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.82rem !important;
}}

/* ── Dataframe ── */
[data-testid="stDataFrame"] {{
    border: 1px solid {T['border']} !important;
    border-radius: 6px !important;
    overflow: hidden !important;
}}

/* ── Divider ── */
hr {{ border-color: {T['border']} !important; }}

/* ── Insight Box ── */
.insight-box {{
    background: {'rgba(201,168,76,0.07)' if st.session_state.dark_mode else 'rgba(184,146,42,0.06)'};
    border: 1px solid {T['gold_dim']};
    border-left: 3px solid {T['gold']};
    padding: 1.25rem 1.5rem;
    border-radius: 0 8px 8px 0;
    margin: 1rem 0;
    font-size: 0.9rem;
    line-height: 1.7;
    color: {T['text_primary']};
}}

.insight-label {{
    font-size: 0.65rem;
    font-weight: 700;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: {T['gold']};
    margin-bottom: 8px;
}}

/* ── SQL Code Block ── */
.stCode {{
    background: {T['bg_card2']} !important;
    border: 1px solid {T['border']} !important;
    border-radius: 6px !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.78rem !important;
}}

/* ── Tabs ── */
[data-testid="stTabs"] {{
    border-bottom: 1px solid {T['border']};
}}

button[data-baseweb="tab"] {{
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.8rem !important;
    font-weight: 500 !important;
    color: {T['text_muted']} !important;
    letter-spacing: 0.04em !important;
}}

button[data-baseweb="tab"][aria-selected="true"] {{
    color: {T['gold']} !important;
    border-bottom-color: {T['gold']} !important;
}}

/* ── Spinner ── */
.stSpinner > div {{ border-top-color: {T['gold']} !important; }}

/* ── Warning / Error / Info ── */
[data-testid="stAlert"] {{
    border-radius: 6px !important;
    font-size: 0.85rem !important;
}}

/* ── Column gaps ── */
[data-testid="stHorizontalBlock"] {{ gap: 1rem; }}

/* ── Main content padding ── */
[data-testid="stMainBlockContainer"] {{
    padding: 0 2rem 2rem !important;
    max-width: 1400px !important;
    margin: 0 auto !important;
}}

/* ── Toggle switch style ── */
.toggle-container {{
    display: flex;
    align-items: center;
    gap: 8px;
    cursor: pointer;
}}

.toggle-icon {{
    font-size: 1rem;
    line-height: 1;
}}

/* ── Data Source Badges ── */
.badge-live {{
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.08em;
    background: rgba(231,76,60,0.15); color: #E74C3C; border: 1px solid rgba(231,76,60,0.3);
}}
.badge-historical {{
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.08em;
    background: rgba(201,168,76,0.15); color: {T['gold']}; border: 1px solid rgba(201,168,76,0.3);
}}
.badge-combined {{
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.08em;
    background: rgba(74,144,217,0.15); color: #4A90D9; border: 1px solid rgba(74,144,217,0.3);
}}
.badge-groq {{
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.08em;
    background: rgba(46,204,113,0.15); color: #2ECC71; border: 1px solid rgba(46,204,113,0.3);
}}
.badge-mistral {{
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.08em;
    background: rgba(155,89,182,0.15); color: #9B59B6; border: 1px solid rgba(155,89,182,0.3);
}}
.badge-yahoo {{
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.08em;
    background: rgba(171,71,188,0.15); color: #AB47BC; border: 1px solid rgba(171,71,188,0.3);
}}
</style>
""", unsafe_allow_html=True)

# ─── SNOWFLAKE CONNECTION ─────────────────────────────────────────────────────
def get_credentials():
    """Try Streamlit Cloud secrets first, then fall back to local .env file"""
    try:
        return {
            "user": st.secrets["SNOWFLAKE_USER"],
            "password": st.secrets["SNOWFLAKE_PASSWORD"],
            "account": st.secrets["SNOWFLAKE_ACCOUNT"],
            "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
        }
    except Exception:
        from dotenv import load_dotenv
        load_dotenv("dot.env")
        return {
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        }

@st.cache_resource
def get_connection():
    try:
        creds = get_credentials()
        return snowflake.connector.connect(
            user=creds["user"],
            password=creds["password"],
            account=creds["account"],
            warehouse=creds["warehouse"],
            database="FINANCE_AI_DB",
            schema="STOCK_DATA",
            client_session_keep_alive=True
        )
    except Exception as e:
        return None

# ─── SMART DATA FETCHER ───────────────────────────────────────────────────────
def standardize_date_column(df, date_col='DATE'):
    """
    Standardize date column by:
    1. Converting to datetime
    2. Removing timezone info
    3. Sorting by date
    
    Args:
        df: DataFrame with date column
        date_col: Name of date column (default 'DATE')
    
    Returns:
        DataFrame with standardized dates
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # Convert to datetime
    df[date_col] = pd.to_datetime(df[date_col])
    
    # Remove timezone if present
    if df[date_col].dt.tz is not None:
        df[date_col] = df[date_col].dt.tz_localize(None)
    
    # Sort by date
    df = df.sort_values(date_col)
    
    return df

def fetch_comparison_data_smart(ticker, start_date, end_date, fmp_api_key):
    """
    Smart data fetcher: tries DB first, falls back to Yahoo Finance API, then FMP API.
    Returns normalized dataframe with DATE, TICKER, CLOSE columns
    """
    import yfinance as yf
    import requests
    
    # Convert dates to datetime
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    # Step 1: Try database first
    try:
        db_query = f"""
            SELECT date, '{ticker}' as ticker, close
            FROM FINANCE_AI_DB.STOCK_DATA.PRICES
            WHERE ticker = '{ticker}'
            AND date >= '{start_date}'
            AND date <= '{end_date}'
            ORDER BY date ASC
        """
        
        db_data = run_query(db_query)
        
        if db_data is not None and not db_data.empty:
            # Database has data! Use it
            db_data.columns = ['DATE', 'TICKER', 'CLOSE']
            db_data = standardize_date_column(db_data)
            return db_data, "database"
            
    except Exception as e:
        pass
    
    # Step 2: Database empty or failed - try Yahoo Finance API
    try:
        # Use Ticker object (works in all yfinance versions)
        stock = yf.Ticker(ticker)
        
        # Get historical data
        yf_data = stock.history(
            start=start_date,
            end=end_date,
            interval='1d'
        )
        
        if not yf_data.empty:
            # Convert to standard format
            result = pd.DataFrame({
                'DATE': yf_data.index,
                'TICKER': ticker,
                'CLOSE': yf_data['Close'].values
            })
            result = standardize_date_column(result)
            return result, "yahoo_api"
            
    except Exception as e:
        pass
    
    # Step 3: Both failed - try FMP API as last resort
    try:
        # Calculate period in days
        days_diff = (end_dt - start_dt).days
        
        # Call FMP API
        fmp_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}"
        params = {
            'apikey': fmp_api_key,
            'from': start_date,
            'to': end_date
        }
        
        response = requests.get(fmp_url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'historical' in data and data['historical']:
                # Convert to dataframe
                fmp_df = pd.DataFrame(data['historical'])
                fmp_df = fmp_df.rename(columns={'date': 'DATE', 'close': 'CLOSE'})
                fmp_df['TICKER'] = ticker
                fmp_df = fmp_df[['DATE', 'TICKER', 'CLOSE']]
                fmp_df = standardize_date_column(fmp_df)
                return fmp_df, "fmp_api"
                
    except Exception as e:
        pass
    
    # All sources failed
    return pd.DataFrame(), "not_found"

def run_query(query):
    conn = get_connection()
    if conn is None:
        raise Exception("Database connection unavailable")
    try:
        cur = conn.cursor()
        cur.execute(query)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        raise Exception(f"Query execution failed: {str(e)}")

def cortex_complete(prompt):
    safe = prompt.replace("'", "\\'").replace("$$", "")
    result = run_query(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', '{safe}') as response
    """)
    return result.iloc[0]['RESPONSE']

# ─── API KEY MANAGEMENT ───────────────────────────────────────────────────────
def get_fmp_api_key():
    """Get Financial Modeling Prep API key"""
    try:
        return st.secrets["FMP_API_KEY"]
    except Exception:
        return os.getenv("FMP_API_KEY", "")

def get_groq_api_key():
    """Get Groq API key for fast LLM"""
    try:
        return st.secrets["GROQ_API_KEY"]
    except Exception:
        return os.getenv("GROQ_API_KEY", "")

fmp_api_key = get_fmp_api_key()
groq_api_key = get_groq_api_key()
groq_available = bool(groq_api_key)

# Track API calls in session state
if "api_calls_today" not in st.session_state:
    st.session_state.api_calls_today = 0
if "groq_calls_today" not in st.session_state:
    st.session_state.groq_calls_today = 0

# ─── SMART LLM ROUTER ────────────────────────────────────────────────────────
def call_llm(prompt, task_type="general"):
    """Try Groq first (fast), fall back to Mistral (reliable)"""
    global groq_available
    
    if groq_available and groq_api_key:
        try:
            headers = {
                "Authorization": f"Bearer {groq_api_key}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 2000
            }
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers, json=payload, timeout=10
            )
            if resp.status_code == 200:
                st.session_state.groq_calls_today += 1
                return resp.json()["choices"][0]["message"]["content"], "groq"
            else:
                groq_available = False
        except Exception:
            groq_available = False
    
    # Fallback to Mistral via Snowflake Cortex
    result = cortex_complete(prompt)
    return result, "mistral"

# ─── LIVE DATA FETCHING ──────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def fetch_live_data(ticker, api_key, period="1y"):
    """Fetch data from Financial Modeling Prep API (stable endpoints)"""
    if not api_key:
        return pd.DataFrame(), "No API key configured"
    
    try:
        # Try stable quote endpoint first (real-time)
        quote_url = "https://financialmodelingprep.com/stable/quote"
        resp = requests.get(quote_url, params={"symbol": ticker.upper(), "apikey": api_key}, timeout=10)
        
        if resp.status_code in (429, 402, 403):
            # FMP blocked (rate limit, premium required, or forbidden) — try Yahoo fallback
            if yfinance_available:
                yf_df, yf_err = fetch_yahoo_data(ticker)
                if yf_err is None and len(yf_df) > 0:
                    return yf_df, None  # Yahoo succeeded silently
            return pd.DataFrame(), f"FMP API: '{ticker.upper()}' requires premium subscription. Free plan only covers major US stocks."
        if resp.status_code != 200:
            # Other HTTP error — try Yahoo fallback
            if yfinance_available:
                yf_df, yf_err = fetch_yahoo_data(ticker)
                if yf_err is None and len(yf_df) > 0:
                    return yf_df, None
            return pd.DataFrame(), f"API error: {resp.status_code} - {resp.text[:100]}"
        
        data = resp.json()
        if not data or len(data) == 0:
            # No data from FMP — try Yahoo fallback
            if yfinance_available:
                yf_df, yf_err = fetch_yahoo_data(ticker)
                if yf_err is None and len(yf_df) > 0:
                    return yf_df, None
            return pd.DataFrame(), f"No data found for {ticker}"
        
        st.session_state.api_calls_today += 1
        
        # Build DataFrame from quote data (single row with current price)
        quote = data[0]
        today_str = datetime.now().strftime("%Y-%m-%d")
        df = pd.DataFrame([{
            "DATE": pd.to_datetime(today_str),
            "TICKER": ticker.upper(),
            "OPEN": quote.get("open", 0),
            "HIGH": quote.get("dayHigh", 0),
            "LOW": quote.get("dayLow", 0),
            "CLOSE": quote.get("price", 0),
            "VOLUME": quote.get("volume", 0),
        }])
        
        for col in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        return df, None
    except requests.exceptions.Timeout:
        if yfinance_available:
            yf_df, yf_err = fetch_yahoo_data(ticker)
            if yf_err is None and len(yf_df) > 0:
                return yf_df, None
        return pd.DataFrame(), "Network timeout"
    except Exception as e:
        if yfinance_available:
            yf_df, yf_err = fetch_yahoo_data(ticker)
            if yf_err is None and len(yf_df) > 0:
                return yf_df, None
        return pd.DataFrame(), str(e)


@st.cache_data(ttl=3600)
def fetch_yahoo_data(ticker):
    """Fallback: Fetch historical data from Yahoo Finance (free, no API key)"""
    try:
        stock = yf.Ticker(ticker.upper())
        # Fetch 1 year of daily data
        hist = stock.history(period="2y")
        
        if hist is None or len(hist) == 0:
            return pd.DataFrame(), f"Yahoo Finance: No data for {ticker}"
        
        df = hist.reset_index()
        df = df.rename(columns={
            "Date": "DATE", "Open": "OPEN", "High": "HIGH",
            "Low": "LOW", "Close": "CLOSE", "Volume": "VOLUME"
        })
        df["TICKER"] = ticker.upper()
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.tz_localize(None)
        df = df[["DATE", "TICKER", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]]
        df = df.sort_values("DATE").reset_index(drop=True)
        
        for col in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        return df, None
    except Exception as e:
        return pd.DataFrame(), f"Yahoo Finance error: {str(e)}"

# ─── UTILITIES & ENHANCEMENTS ────────────────────────────────────────────────
def extract_roi_params(prompt):
    prompt_llm = f"""Extract ROI investment parameters from this text: '{prompt}'
Return ONLY a JSON object with strictly these keys:
- amount: (float, the dollar amount invested)
- ticker: (string, the stock ticker symbol)
- start_date: (string, the start date or year, e.g. '2020' or '2020-01-01')
If not an investment query or missing info, return {{"error": "not_roi"}}
Do not include markdown blocks, just the JSON string."""
    result, _ = call_llm(prompt_llm, "intent_detection")
    try:
        import re
        json_match = re.search(r'\{.*\}', result.replace('\n', ''), re.DOTALL)
        if json_match:
            return json.loads(json_match.group(0))
        return json.loads(result.strip())
    except:
        return {"error": "parsing_failed"}

def handle_roi_query(roi_params):
    if not yfinance_available:
        return None, "Yahoo Finance not available for historical lookup."
    try:
        amount = float(roi_params['amount'])
        ticker = roi_params['ticker'].upper()
        start_date_str = str(roi_params['start_date'])
        
        stock = yf.Ticker(ticker)
        start_date_parsed = pd.to_datetime(start_date_str)
        if len(start_date_str) == 4: # Just a year
            start_date_parsed = pd.to_datetime(f"{start_date_str}-01-02")
            
        hist = stock.history(start=start_date_parsed.strftime("%Y-%m-%d"), end=(start_date_parsed + pd.Timedelta(days=14)).strftime("%Y-%m-%d"))
        if hist.empty:
            return None, f"No data available for {ticker} around {start_date_str}"
        start_price = float(hist['Close'].iloc[0])
        
        current = stock.history(period="1d")
        if current.empty:
            return None, "No current data available."
        end_price = float(current['Close'].iloc[-1])
        
        shares = amount / start_price
        current_value = shares * end_price
        return_val = current_value - amount
        roi_pct = (return_val / amount) * 100
        
        return {
            "amount": amount, "ticker": ticker, "start_date": start_date_parsed.strftime("%Y-%m-%d"),
            "start_price": start_price, "end_price": end_price, "shares": shares,
            "current_value": current_value, "return_val": return_val, "roi_pct": roi_pct
        }, None
    except Exception as e:
        return None, str(e)

# ─── DATABASE CHECKS ─────────────────────────────────────────────────────────
snowflake_available = True

def check_ticker_in_db(ticker):
    """Check if ticker exists and get last date. Returns (exists, last_date, count, db_ok)"""
    global snowflake_available
    conn = get_connection()
    if conn is None:
        snowflake_available = False
        return False, None, 0, False
        
    try:
        result = run_query(f"""
            SELECT COUNT(*) as cnt, MAX(date) as last_date
            FROM FINANCE_AI_DB.STOCK_DATA.PRICES
            WHERE ticker = '{ticker.upper()}'
        """)
        snowflake_available = True
        row = result.iloc[0]
        cnt = int(row['CNT'])
        last_date = row['LAST_DATE']
        if cnt > 0 and last_date:
            return True, pd.to_datetime(str(last_date)), cnt, True
        return False, None, 0, True
    except Exception as e:
        snowflake_available = False
        return False, None, 0, False  # DB is down

# ─── TICKER EXTRACTION ───────────────────────────────────────────────────────
def extract_tickers_from_question(question):
    """Use LLM to extract ticker symbols from natural language"""
    prompt = f"""Extract stock ticker symbols from this question. Return ONLY a JSON array of uppercase ticker symbols, nothing else.
If the question mentions company names, convert to tickers. If it mentions 'tech stocks', return: ["AAPL","MSFT","GOOGL","META","NVDA","TSLA","AMZN","NFLX","AMD","INTC"]
If it mentions 'banking stocks', return: ["JPM","BAC","GS","MS","WFC","C"]
If no specific stocks are mentioned, return an empty array [].
Max 10 tickers.

Question: {question}

Return ONLY the JSON array:"""
    
    result, model_used = call_llm(prompt, "ticker_extraction")
    
    try:
        # Parse the JSON array from the response
        result = result.strip()
        if "[" in result:
            arr_str = result[result.index("["):result.index("]")+1]
            tickers = json.loads(arr_str)
            return tickers[:10], model_used
    except:
        pass
    return [], model_used

# ─── TICKER SUGGESTIONS ──────────────────────────────────────────────────────
def suggest_similar_tickers(invalid_ticker):
    """Suggest alternatives for typos"""
    prompt = f"""The user searched for stock ticker "{invalid_ticker}" which doesn't exist.
Suggest 3 most likely correct ticker symbols they might have meant.
Return ONLY a JSON array of objects with 'ticker' and 'name' keys.
Example: [{{"ticker":"AAPL","name":"Apple Inc."}},{{"ticker":"AMZN","name":"Amazon"}}]

Return ONLY the JSON array:"""
    
    result, _ = call_llm(prompt, "suggestions")
    try:
        result = result.strip()
        if "[" in result:
            arr_str = result[result.index("["):result.rindex("]")+1]
            return json.loads(arr_str)
    except:
        pass
    return [{"ticker": "AAPL", "name": "Apple Inc."}, 
            {"ticker": "MSFT", "name": "Microsoft"}, 
            {"ticker": "GOOGL", "name": "Alphabet"}]

# ─── SMART DATA ROUTER ───────────────────────────────────────────────────────
def smart_data_router(tickers, fmp_key):
    """Decide data source for each ticker and fetch data"""
    results = {}
    today = datetime.now().date()
    
    for ticker in tickers:
        exists, last_date, row_count, db_ok = check_ticker_in_db(ticker)
        
        if not db_ok:
            # Snowflake is down — route directly to API
            if fmp_key:
                api_df, api_err = fetch_live_data(ticker, fmp_key)
                if api_err is None and len(api_df) > 0:
                    results[ticker] = {
                        "source": "api",
                        "badge": "🔴 LIVE DATA",
                        "last_date": None,
                        "rows": 0,
                        "api_data": api_df,
                        "error": None
                    }
                else:
                    # FMP failed — try Yahoo Finance fallback
                    if yfinance_available:
                        yf_df, yf_err = fetch_yahoo_data(ticker)
                        if yf_err is None and len(yf_df) > 0:
                            results[ticker] = {
                                "source": "api",
                                "badge": "🟡 YAHOO",
                                "last_date": None,
                                "rows": 0,
                                "api_data": yf_df,
                                "error": None
                            }
                            continue
                    suggestions = suggest_similar_tickers(ticker)
                    results[ticker] = {
                        "source": "not_found",
                        "badge": "❌ NOT FOUND",
                        "last_date": None,
                        "rows": 0,
                        "api_data": None,
                        "error": api_err or f"Ticker '{ticker}' not found",
                        "suggestions": suggestions
                    }
            else:
                # No FMP key — try Yahoo Finance directly
                if yfinance_available:
                    yf_df, yf_err = fetch_yahoo_data(ticker)
                    if yf_err is None and len(yf_df) > 0:
                        results[ticker] = {
                            "source": "api",
                            "badge": "🟡 YAHOO",
                            "last_date": None,
                            "rows": 0,
                            "api_data": yf_df,
                            "error": None
                        }
                        continue
                results[ticker] = {
                    "source": "not_found",
                    "badge": "❌ NOT FOUND",
                    "last_date": None,
                    "rows": 0,
                    "api_data": None,
                    "error": "Database offline and no API available"
                }
        elif exists and last_date:
            days_old = (today - last_date.date()).days
            
            if days_old <= 30:
                results[ticker] = {
                    "source": "database",
                    "badge": "📊 HISTORICAL",
                    "last_date": last_date,
                    "rows": row_count,
                    "api_data": None,
                    "error": None
                }
            else:
                if fmp_key:
                    api_df, api_err = fetch_live_data(ticker, fmp_key)
                    if api_err is None and len(api_df) > 0:
                        results[ticker] = {
                            "source": "combined",
                            "badge": "🔄 COMBINED",
                            "last_date": last_date,
                            "rows": row_count,
                            "api_data": api_df,
                            "error": None
                        }
                    else:
                        if yfinance_available:
                            yf_df, yf_err = fetch_yahoo_data(ticker)
                            if yf_err is None and len(yf_df) > 0:
                                results[ticker] = {
                                    "source": "combined",
                                    "badge": "🔄 COMBINED (YAHOO)",
                                    "last_date": last_date,
                                    "rows": row_count,
                                    "api_data": yf_df,
                                    "error": None
                                }
                                continue
                        results[ticker] = {
                            "source": "database",
                            "badge": "📊 HISTORICAL",
                            "last_date": last_date,
                            "rows": row_count,
                            "api_data": None,
                            "error": api_err
                        }
                else:
                    results[ticker] = {
                        "source": "database",
                        "badge": "📊 HISTORICAL",
                        "last_date": last_date,
                        "rows": row_count,
                        "api_data": None,
                        "error": "No API key configured"
                    }
        else:
            # Not in DB — try API
            if fmp_key:
                api_df, api_err = fetch_live_data(ticker, fmp_key)
                if api_err is None and len(api_df) > 0:
                    results[ticker] = {
                        "source": "api",
                        "badge": "🔴 LIVE DATA",
                        "last_date": None,
                        "rows": 0,
                        "api_data": api_df,
                        "error": None
                    }
                else:
                    # FMP failed — try Yahoo Finance fallback
                    if yfinance_available:
                        yf_df, yf_err = fetch_yahoo_data(ticker)
                        if yf_err is None and len(yf_df) > 0:
                            results[ticker] = {
                                "source": "api",
                                "badge": "🟡 YAHOO",
                                "last_date": None,
                                "rows": 0,
                                "api_data": yf_df,
                                "error": None
                            }
                            continue
                    suggestions = suggest_similar_tickers(ticker)
                    results[ticker] = {
                        "source": "not_found",
                        "badge": "❌ NOT FOUND",
                        "last_date": None,
                        "rows": 0,
                        "api_data": None,
                        "error": f"Ticker '{ticker}' not found",
                        "suggestions": suggestions
                    }
            else:
                # No FMP key — try Yahoo Finance directly
                if yfinance_available:
                    yf_df, yf_err = fetch_yahoo_data(ticker)
                    if yf_err is None and len(yf_df) > 0:
                        results[ticker] = {
                            "source": "api",
                            "badge": "🟡 YAHOO",
                            "last_date": None,
                            "rows": 0,
                            "api_data": yf_df,
                            "error": None
                        }
                        continue
                results[ticker] = {
                    "source": "not_found",
                    "badge": "❌ NOT FOUND",
                    "last_date": None,
                    "rows": 0,
                    "api_data": None,
                    "error": f"'{ticker}' not found in any data source"
                }
    
    return results


# ─── DASHBOARD CHART HELPER ──────────────────────────────────────────────────
def show_stock_chart(ticker, days, theme, route_info):
    """Display a candlestick + volume chart using combined DB + live Yahoo data."""
    try:
        # ── 1. Always fetch real-time history from Yahoo Finance ──
        yahoo_df = pd.DataFrame()
        if yfinance_available:
            try:
                yf_data, yf_err = fetch_yahoo_data(ticker)
                if yf_err is None and len(yf_data) > 0:
                    yahoo_df = yf_data.copy()
                    yahoo_df.columns = [c.upper() for c in yahoo_df.columns]
            except Exception:
                pass

        # ── 2. Also grab API data from route_info (FMP quote) ──
        api_df = pd.DataFrame()
        if route_info.get("api_data") is not None and len(route_info["api_data"]) > 0:
            api_df = route_info["api_data"].copy()
            api_df.columns = [c.upper() for c in api_df.columns]

        # ── 3. Grab DB data ──
        db_df = pd.DataFrame()
        if route_info.get("source") in ("database", "combined"):
            try:
                db_df = run_query(f"""
                    SELECT date, open, high, low, close, volume
                    FROM FINANCE_AI_DB.STOCK_DATA.PRICES
                    WHERE ticker = '{ticker.upper()}'
                    ORDER BY date DESC
                    LIMIT 2000
                """)
                if db_df is not None and len(db_df) > 0:
                    db_df.columns = [c.upper() for c in db_df.columns]
                else:
                    db_df = pd.DataFrame()
            except Exception:
                db_df = pd.DataFrame()

        # ── 4. Combine all sources (Yahoo preferred for recency) ──
        db_df = standardize_date_column(db_df)
        api_df = standardize_date_column(api_df)
        yahoo_df = standardize_date_column(yahoo_df)
        
        frames = [df for df in [db_df, api_df, yahoo_df] if not df.empty]
        if not frames:
            st.warning(f"No data available for {ticker}")
            return

        data = pd.concat(frames, ignore_index=True)
        data['DATE'] = pd.to_datetime(data['DATE'])
        data = data.drop_duplicates(subset=['DATE'], keep='last')
        data = data.sort_values('DATE').reset_index(drop=True)

        # ── 5. Filter: take the last N trading days of available data ──
        if days is not None:
            data = data.tail(days)

        if data.empty:
            st.warning(f"No data available for {ticker} in this period")
            return

        start_date = data['DATE'].min().strftime('%b %d, %Y')
        end_date = data['DATE'].max().strftime('%b %d, %Y')
        st.caption(f"📊 {len(data)} trading days | {start_date} → {end_date}")

        # ── 6. Chart ──
        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            vertical_spacing=0.04, row_heights=[0.7, 0.3]
        )

        fig.add_trace(go.Candlestick(
            x=data['DATE'],
            open=data['OPEN'], high=data['HIGH'],
            low=data['LOW'], close=data['CLOSE'],
            name=ticker,
            increasing_line_color=theme['green'],
            decreasing_line_color=theme['red'],
        ), row=1, col=1)

        vol_colors = [theme['green'] if c >= o else theme['red']
                      for c, o in zip(data['CLOSE'], data['OPEN'])]
        fig.add_trace(go.Bar(
            x=data['DATE'], y=data['VOLUME'],
            name='Volume', marker_color=vol_colors, opacity=0.5,
        ), row=2, col=1)

        fig.update_layout(
            paper_bgcolor=theme['plotly_paper'],
            plot_bgcolor=theme['plotly_plot'],
            font=dict(family='DM Sans', color=theme['plotly_text'], size=11),
            xaxis=dict(gridcolor=theme['plotly_grid']),
            yaxis=dict(gridcolor=theme['plotly_grid'], title='Price',
                       tickprefix='$', tickfont=dict(family='JetBrains Mono', size=10)),
            xaxis2=dict(gridcolor=theme['plotly_grid']),
            yaxis2=dict(gridcolor=theme['plotly_grid'], title='Volume',
                        tickfont=dict(family='JetBrains Mono', size=9)),
            xaxis_rangeslider_visible=False,
            height=500,
            margin=dict(l=10, r=10, t=20, b=10),
            legend=dict(bgcolor='rgba(0,0,0,0)', bordercolor=theme['border'], borderwidth=1),
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Chart error for {ticker}: {str(e)}")

# ─── SMART TICKER LOOKUP ──────────────────────────────────────────────────
def smart_ticker_lookup(search_input):
    """Convert company names or partial matches to valid ticker symbols."""
    if not search_input:
        return None

    cleaned = search_input.strip()
    upper = cleaned.upper()

    # If it looks like a ticker already (1-5 uppercase letters / with dot), return it
    if cleaned.replace('.', '').isalpha() and len(cleaned) <= 6:
        return upper

    # Company name → ticker mapping
    ticker_map = {
        # Tech
        'apple': 'AAPL', 'microsoft': 'MSFT', 'google': 'GOOGL', 'googl': 'GOOGL', 'goog': 'GOOG',
        'alphabet': 'GOOGL', 'amazon': 'AMZN', 'meta': 'META',
        'facebook': 'META', 'nvidia': 'NVDA', 'tesla': 'TSLA',
        'netflix': 'NFLX', 'amd': 'AMD', 'intel': 'INTC',
        'adobe': 'ADBE', 'salesforce': 'CRM', 'oracle': 'ORCL',
        'palantir': 'PLTR', 'snowflake': 'SNOW', 'uber': 'UBER',
        'spotify': 'SPOT', 'snap': 'SNAP', 'snapchat': 'SNAP',
        'roblox': 'RBLX', 'shopify': 'SHOP', 'airbnb': 'ABNB',
        'crowdstrike': 'CRWD', 'datadog': 'DDOG', 'arm': 'ARM',
        'broadcom': 'AVGO', 'qualcomm': 'QCOM', 'ibm': 'IBM',
        'taiwan semiconductor': 'TSM', 'tsmc': 'TSM',
        # Finance
        'jpmorgan': 'JPM', 'jp morgan': 'JPM', 'chase': 'JPM',
        'bank of america': 'BAC', 'bofa': 'BAC',
        'goldman sachs': 'GS', 'goldman': 'GS',
        'morgan stanley': 'MS', 'visa': 'V', 'mastercard': 'MA',
        'paypal': 'PYPL', 'square': 'SQ', 'block': 'SQ',
        'berkshire': 'BRK.B', 'berkshire hathaway': 'BRK.B',
        'wells fargo': 'WFC', 'citigroup': 'C', 'citi': 'C',
        'blackrock': 'BLK', 'american express': 'AXP',
        # Retail / Consumer
        'walmart': 'WMT', 'target': 'TGT', 'costco': 'COST',
        'home depot': 'HD', 'lowes': 'LOW', "lowe's": 'LOW',
        'nike': 'NKE', 'starbucks': 'SBUX', 'mcdonalds': 'MCD',
        "mcdonald's": 'MCD', 'coca cola': 'KO', 'coca-cola': 'KO',
        'coke': 'KO', 'pepsi': 'PEP', 'pepsico': 'PEP',
        'disney': 'DIS', 'walt disney': 'DIS',
        # Healthcare
        'johnson': 'JNJ', 'johnson & johnson': 'JNJ', 'j&j': 'JNJ',
        'unitedhealth': 'UNH', 'pfizer': 'PFE', 'eli lilly': 'LLY',
        'lilly': 'LLY', 'abbvie': 'ABBV', 'merck': 'MRK',
        'amgen': 'AMGN', 'gilead': 'GILD', 'moderna': 'MRNA',
        # Energy
        'exxon': 'XOM', 'exxonmobil': 'XOM', 'exxon mobil': 'XOM',
        'chevron': 'CVX', 'conocophillips': 'COP', 'shell': 'SHEL',
        # Industrial
        'boeing': 'BA', 'caterpillar': 'CAT', 'ge': 'GE',
        'general electric': 'GE', 'honeywell': 'HON',
        'lockheed': 'LMT', 'lockheed martin': 'LMT',
        # Other
        'procter': 'PG', 'procter & gamble': 'PG', 'p&g': 'PG',
        'att': 'T', 'at&t': 'T', 'verizon': 'VZ', 'comcast': 'CMCSA',
    }

    lower = cleaned.lower()

    # Exact match
    if lower in ticker_map:
        return ticker_map[lower]

    # Partial / substring match
    for name, ticker in ticker_map.items():
        if lower in name or name in lower:
            return ticker

    # Fallback: treat input as a ticker symbol
    return upper

# ─── CHAT CONTEXT BUILDERS ─────────────────────────────────────────────────
def build_comparison_chat_context(tickers, comp_df, route_results, data_sources=None):
    """Build detailed context for comparison chatbot."""
    parts = [f"You are comparing these stocks: {', '.join(tickers)}", "", "CURRENT DATA:"]
    for _, row in comp_df.iterrows():
        tk = row['TICKER']
        price = row['PRICE']
        return_1y = row['1Y_RETURN']
        volume = row['VOLUME']
        
        # Add data source if available
        source_info = ""
        if data_sources and tk in data_sources:
            source = data_sources[tk]
            if source == 'yahoo_api':
                source_info = " (real-time via Yahoo Finance)"
            elif source == 'fmp_api':
                source_info = " (real-time via FMP API)"
            elif source == 'database':
                source_info = " (historical database)"
                
        parts.append(f"- {tk}: ${price:.2f}, 1Y Return: {return_1y:+.2f}%, Volume: {volume:,.0f}{source_info}")
        
    parts.append("")
    parts.append("DATA SOURCES:")
    for tk in tickers:
        src = (data_sources.get(tk) if data_sources else None) or route_results.get(tk, {}).get('source', 'unknown')
        parts.append(f"- {tk}: {src}")
    parts.append("")
    try:
        for tk in tickers:
            stats = run_query(f"""
                SELECT MIN(low) as all_time_low, MAX(high) as all_time_high, AVG(close) as avg_price
                FROM FINANCE_AI_DB.STOCK_DATA.PRICES WHERE ticker = '{tk}' LIMIT 1
            """)
            if stats is not None and not stats.empty:
                r = stats.iloc[0]
                parts.append(f"{tk} Historical: ATH ${r['ALL_TIME_HIGH']:.2f}, ATL ${r['ALL_TIME_LOW']:.2f}, Avg ${r['AVG_PRICE']:.2f}")
    except Exception:
        pass
    return "\n".join(parts)

def build_single_stock_chat_context(ticker, latest, route_info):
    """Build detailed context for single-stock chatbot."""
    parts = [f"Stock: {ticker}", f"Current Price: ${float(latest['CLOSE']):.2f}",
             f"Open: ${float(latest['OPEN']):.2f}", f"High: ${float(latest['HIGH']):.2f}",
             f"Low: ${float(latest['LOW']):.2f}", f"Volume: {float(latest['VOLUME']):,.0f}",
             f"Date: {latest['DATE']}", f"Data Source: {route_info.get('source', 'unknown')}", ""]
    try:
        hist = run_query(f"""
            SELECT MIN(low) as atl, MAX(high) as ath, AVG(close) as avg_price,
                   MIN(date) as first_date, MAX(date) as last_date
            FROM FINANCE_AI_DB.STOCK_DATA.PRICES WHERE ticker = '{ticker}'
        """)
        if hist is not None and not hist.empty:
            h = hist.iloc[0]
            parts.append(f"Historical: ATH ${h['ATH']:.2f}, ATL ${h['ATL']:.2f}, Avg ${h['AVG_PRICE']:.2f}")
            parts.append(f"Data range: {h['FIRST_DATE']} to {h['LAST_DATE']}")
    except Exception:
        pass
    return "\n".join(parts)

# ─── TOP NAV ─────────────────────────────────────────────────────────────────
nav_col1, nav_col2, nav_col3 = st.columns([2, 4, 2])

with nav_col1:
    st.markdown(f"""
    <div class="nav-logo">
        <div>
            <div class="nav-logo-text">FinSight<span style="color:{T['text_muted']}">AI</span></div>
            <div class="nav-logo-tag">Quantitative Intelligence Platform</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

with nav_col2:
    nav_options = ["📊 Analysis", "🔬 Research", "📊 Dashboard"]
    if "nav_page" not in st.session_state:
        st.session_state.nav_page = "📊 Analysis"
    
    cols = st.columns(len(nav_options))
    for i, option in enumerate(nav_options):
        with cols[i]:
            is_active = st.session_state.nav_page == option
            if st.button(
                option,
                key=f"nav_{i}",
                type="primary" if is_active else "secondary"
            ):
                st.session_state.nav_page = option
                st.rerun()

with nav_col3:
    c1, c2 = st.columns([1, 1])
    with c1:
        st.markdown(f"""
        <div style="display:flex; justify-content:flex-end; align-items:center; height:64px;">
            <div class="nav-pill"><div class="nav-dot"></div>Live</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown(f"<div style='display:flex; align-items:center; justify-content:center; height:64px;'>", unsafe_allow_html=True)
        mode_label = "☀️ Light" if st.session_state.dark_mode else "🌙 Dark"
        if st.button(mode_label, key="theme_toggle"):
            st.session_state.dark_mode = not st.session_state.dark_mode
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<div style='height:1px; background:" + T['border'] + "; margin-bottom:1.5rem;'></div>", unsafe_allow_html=True)

# ─── SIDEBAR STATUS PANEL ─────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"### ⚙️ System Status")
    
    # API Status
    st.markdown("**Data Sources**")
    if fmp_api_key:
        # Validate FMP API tier by testing a premium stock
        try:
            test_resp = requests.get(
                "https://financialmodelingprep.com/stable/quote",
                params={"symbol": "KKR", "apikey": fmp_api_key}, timeout=5
            )
            if test_resp.status_code == 200 and test_resp.json():
                st.success("🟢 FMP API: Premium (all stocks)")
            elif test_resp.status_code in (402, 403):
                st.warning("🟡 FMP API: Free tier (using Yahoo fallback for premium stocks)")
            else:
                st.warning("🟡 FMP API: Limited (Yahoo fallback active)")
        except Exception:
            st.warning("🟡 FMP API: Connection issue (Yahoo fallback active)")
    else:
        st.info("🔵 FMP API: No key — using Yahoo Finance for live data")
    
    if yfinance_available:
        st.success("🟢 Yahoo Finance: Active (free fallback)")
    else:
        st.warning("🟡 Yahoo Finance: Not installed")
    
    st.markdown("**AI Models**")
    if groq_available:
        st.success("🟢 Groq (Fast AI): Active")
    else:
        st.warning("🟡 Groq: Offline → Using Mistral")
    st.success("🟢 Mistral (Cortex): Always Active")
    
    st.divider()
    
    # Usage counters
    st.markdown("**Daily Usage**")
    st.caption(f"📡 FMP API: {st.session_state.api_calls_today}/250 calls")
    st.caption(f"⚡ Groq AI: {st.session_state.groq_calls_today}/1800 calls")
    
    st.divider()
    st.caption("📊 DB: 29.7M records (1962-2023)")
    st.caption("🔴 Live: FMP API (real-time)")
    st.caption("⚡ Fast AI: Groq (0.3s)")
    st.caption("🛡️ Reliable AI: Mistral (2-3s)")


current_page = st.session_state.get("nav_page", "📊 Analysis")

if current_page == "🔬 Research":
    st.markdown("### 🔬 AI Research Terminal")
    st.info("Deep-dive analysis powered by Cortex AI")
    research_topic = st.selectbox(
        "Select a stock to research deeply",
        options=[
            "AAPL","MSFT","GOOGL","GOOG","AMZN","META","NVDA","TSLA","NFLX","AMD",
            "JPM","BAC","GS","MS","WFC","C","BLK","AXP","V","MA","PYPL","SQ",
            "JNJ","PFE","UNH","ABBV","MRK","LLY","BMY","AMGN","GILD","CVS",
            "XOM","CVX","COP","SLB","EOG","MPC","PSX","VLO","OXY","HAL",
            "WMT","TGT","COST","HD","LOW","NKE","SBUX","MCD","YUM",
            "BA","CAT","GE","HON","MMM","LMT","RTX","NOC","DE","EMR",
            "T","VZ","CMCSA","DIS","PARA","WBD","SPOT","SNAP",
            "INTC","QCOM","TXN","MU","AMAT","LRCX","KLAC","MRVL","AVGO","ARM",
            "CRM","ORCL","SAP","ADBE","NOW","SNOW","PLTR","DDOG","ZS","CRWD",
            "SPY","QQQ","DIA","IWM","VTI","GLD","SLV","USO","TLT","HYG",
            "BRK.B","IBM","UBER","LYFT","ABNB","DASH","RBLX","U","RIVN","LCID"
        ],
        index=0
    )
    if st.button("🔍 Generate Research Report"):
        with st.spinner("Generating comprehensive research..."):
            try:
                data = run_query(f"""
                    SELECT YEAR(date) as year,
                           AVG(close) as avg_close,
                           MAX(high) as year_high,
                           MIN(low) as year_low,
                           SUM(volume) as total_volume
                    FROM FINANCE_AI_DB.STOCK_DATA.PRICES
                    WHERE ticker = '{research_topic.upper()}'
                    GROUP BY YEAR(date)
                    ORDER BY year
                """)
                st.dataframe(data, use_container_width=True)
                
                report = cortex_complete(f"""
                You are a senior equity research analyst. Write a comprehensive research 
                report on {research_topic.upper()} based on this historical data:
                {data.to_string()}
                
                Include: Executive Summary, Historical Performance Analysis, 
                Key Trends, Risk Factors, and Investment Outlook.
                Format with clear sections.
                """)
                st.markdown(f"""
                <div class="insight-box">
                    <div class="insight-label">⬡ AI Research Report — {research_topic.upper()}</div>
                    {report}
                </div>
                """, unsafe_allow_html=True)
                
                csv = data.to_csv(index=False)
                st.download_button("📥 Download Research Data", csv, 
                                 f"{research_topic}_research.csv", "text/csv")
            except Exception as e:
                st.error(f"Error: {e}")
    st.stop()

if current_page == "📊 Dashboard":
    st.markdown("### 📊 Stock Intelligence Dashboard")
    col1, col2 = st.columns([3, 1])

    with col1:
        search_input = st.text_input(
            "🔍 Search Stock Ticker",
            placeholder="Type ticker symbol (e.g., AAPL, TSLA, GOOGL)...",
            help="Enter any stock ticker symbol"
        )
        st.caption("💡 Popular: AAPL, MSFT, GOOGL, AMZN, META, NVDA, TSLA, NFLX, JPM, TSMC")

    with col2:
        compare_mode = st.checkbox("Compare Mode", help="Compare multiple stocks side-by-side")

    # Set dashboard_ticker from search input using smart lookup
    if search_input:
        dashboard_ticker = smart_ticker_lookup(search_input)
    else:
        dashboard_ticker = None

    # Show empty state if no ticker entered
    if not dashboard_ticker:
        st.info("👆 Enter a stock ticker symbol to view its dashboard")
        st.stop()

    if compare_mode:
        # ── Comparison mode ──────────────────────────────────────────────
        st.markdown("**🔍 Build Your Comparison**")

        # Initialize comparison list with searched ticker
        if 'comparison_tickers' not in st.session_state:
            st.session_state.comparison_tickers = []
        if dashboard_ticker and dashboard_ticker not in st.session_state.comparison_tickers:
            st.session_state.comparison_tickers.insert(0, dashboard_ticker)

        col_add, col_list = st.columns([2, 3])

        with col_add:
            add_ticker = st.text_input(
                "Add ticker to comparison",
                placeholder="Type ticker (e.g., KKR, BX, TSMC)...",
                help="Add any stock ticker symbol",
                key="comp_add_input"
            )
            if add_ticker:
                add_ticker = smart_ticker_lookup(add_ticker)
            if st.button("➕ Add to Comparison") and add_ticker:
                if len(st.session_state.comparison_tickers) >= 4:
                    st.warning("Maximum 4 stocks allowed")
                elif add_ticker in st.session_state.comparison_tickers:
                    st.warning(f"{add_ticker} is already in the list")
                else:
                    st.session_state.comparison_tickers.append(add_ticker)
                    st.rerun()

            # Quick-add popular tickers
            st.caption("Quick add:")
            qa1, qa2, qa3, qa4 = st.columns(4)
            for btn_col, tk in zip([qa1, qa2, qa3, qa4], ["AAPL", "MSFT", "GOOGL", "NVDA"]):
                with btn_col:
                    if st.button(tk, key=f"qa_{tk}", use_container_width=True):
                        if tk not in st.session_state.comparison_tickers and len(st.session_state.comparison_tickers) < 4:
                            st.session_state.comparison_tickers.append(tk)
                            st.rerun()

        with col_list:
            if st.session_state.comparison_tickers:
                st.markdown("**Stocks in Comparison:**")
                for i, ticker in enumerate(st.session_state.comparison_tickers):
                    col_ticker, col_remove = st.columns([3, 1])
                    with col_ticker:
                        st.markdown(f"**{i+1}.** {ticker}")
                    with col_remove:
                        if st.button("✕", key=f"remove_{ticker}"):
                            st.session_state.comparison_tickers.remove(ticker)
                            st.rerun()
            else:
                st.caption("No stocks added yet")

        compare_tickers = st.session_state.comparison_tickers

        if len(compare_tickers) < 2:
            st.info("👆 Add at least 2 stocks to start comparison")
            st.stop()

        with st.spinner("Fetching comparison data…"):
            route_results = smart_data_router(compare_tickers, fmp_api_key)
            comp_rows = []
            for tk in compare_tickers:
                info = route_results.get(tk)
                if info is None or info["source"] == "not_found":
                    continue

                # Latest price row
                if info.get("api_data") is not None and len(info["api_data"]) > 0:
                    row = info["api_data"].iloc[-1]
                else:
                    try:
                        db_row = run_query(f"""
                            SELECT * FROM FINANCE_AI_DB.STOCK_DATA.PRICES
                            WHERE ticker = '{tk.upper()}' ORDER BY date DESC LIMIT 1
                        """)
                        if db_row is None or len(db_row) == 0:
                            continue
                        row = db_row.iloc[0]
                    except Exception:
                        continue

                price = float(row.get('CLOSE', 0))
                high  = float(row.get('HIGH', 0))
                low   = float(row.get('LOW', 0))
                vol   = float(row.get('VOLUME', 0))

                # 1-year return
                yr_return = 0.0
                try:
                    yr_data = run_query(f"""
                        SELECT close FROM FINANCE_AI_DB.STOCK_DATA.PRICES
                        WHERE ticker = '{tk.upper()}'
                          AND date <= DATEADD(day, -365, CURRENT_DATE())
                        ORDER BY date DESC LIMIT 1
                    """)
                    if yr_data is not None and len(yr_data) > 0:
                        old_price = float(yr_data.iloc[0]['CLOSE'])
                        if old_price > 0:
                            yr_return = ((price - old_price) / old_price) * 100
                except Exception:
                    pass

                comp_rows.append({
                    "TICKER": tk.upper(),
                    "PRICE": price,
                    "HIGH": high,
                    "LOW": low,
                    "VOLUME": vol,
                    "1Y_RETURN": yr_return,
                })

        if not comp_rows:
            st.error("Could not fetch data for the selected tickers.")
            st.stop()

        comp_df = pd.DataFrame(comp_rows)

        # ── Performance Summary ──────────────────────────────────────────
        best_performer = comp_df.loc[comp_df['1Y_RETURN'].idxmax()]
        worst_performer = comp_df.loc[comp_df['1Y_RETURN'].idxmin()]

        summary_col1, summary_col2, summary_col3 = st.columns(3)

        with summary_col1:
            st.markdown(f"""
            <div style="background:{T['green']}20; padding:1rem; border-radius:8px; border:1px solid {T['green']};">
                <div style="color:{T['text_muted']}; font-size:0.9rem;">Best Performer</div>
                <div style="color:{T['green']}; font-size:1.5rem; font-weight:700;">{best_performer['TICKER']}</div>
                <div style="color:{T['text_primary']}; font-size:1.1rem;">+{best_performer['1Y_RETURN']:.2f}%</div>
            </div>
            """, unsafe_allow_html=True)

        with summary_col2:
            avg_return = comp_df['1Y_RETURN'].mean()
            st.markdown(f"""
            <div style="background:{T['bg_card']}; padding:1rem; border-radius:8px; border:1px solid {T['border']};">
                <div style="color:{T['text_muted']}; font-size:0.9rem;">Average Return</div>
                <div style="color:{T['gold']}; font-size:1.5rem; font-weight:700;">Portfolio</div>
                <div style="color:{T['text_primary']}; font-size:1.1rem;">{avg_return:+.2f}%</div>
            </div>
            """, unsafe_allow_html=True)

        with summary_col3:
            st.markdown(f"""
            <div style="background:{T['red']}20; padding:1rem; border-radius:8px; border:1px solid {T['red']};">
                <div style="color:{T['text_muted']}; font-size:0.9rem;">Worst Performer</div>
                <div style="color:{T['red']}; font-size:1.5rem; font-weight:700;">{worst_performer['TICKER']}</div>
                <div style="color:{T['text_primary']}; font-size:1.1rem;">{worst_performer['1Y_RETURN']:+.2f}%</div>
            </div>
            """, unsafe_allow_html=True)

        st.divider()

        # ── Performance cards ────────────────────────────────────────────
        card_cols = st.columns(len(comp_df))
        for idx, (_, r) in enumerate(comp_df.iterrows()):
            ret = r['1Y_RETURN']
            arrow = "▲" if ret >= 0 else "▼"
            ret_color = T['green'] if ret >= 0 else T['red']
            with card_cols[idx]:
                st.markdown(f"""
                <div style="background:{T['bg_card']}; border:1px solid {T['border']};
                            border-radius:10px; padding:1.2rem 1rem; text-align:center;">
                    <div style="font-family:'Playfair Display',serif; font-size:1.3rem;
                                font-weight:700; color:{T['gold']};">{r['TICKER']}</div>
                    <div style="font-size:2rem; font-weight:700; color:{T['text_primary']};
                                margin:0.4rem 0;">${r['PRICE']:,.2f}</div>
                    <div style="font-size:0.95rem; font-weight:600; color:{ret_color};">
                        {arrow} {abs(ret):.2f}%
                    </div>
                    <div style="font-size:0.72rem; color:{T['text_muted']}; margin-top:0.5rem;">
                        H ${r['HIGH']:,.2f} &nbsp;·&nbsp; L ${r['LOW']:,.2f}
                    </div>
                    <div style="font-size:0.72rem; color:{T['text_muted']};">
                        Vol {r['VOLUME']:,.0f}
                    </div>
                </div>
                """, unsafe_allow_html=True)

        st.divider()

        # ── Comparison Timeline Selector ─────────────────────────────────
        st.markdown("**📅 Comparison Time Period**")
        comp_c1, comp_c2, comp_c3, comp_c4, comp_c5 = st.columns(5)
        with comp_c1:
            comp_1m = st.button("1M", key="comp_1m", use_container_width=True)
        with comp_c2:
            comp_3m = st.button("3M", key="comp_3m", use_container_width=True)
        with comp_c3:
            comp_6m = st.button("6M", key="comp_6m", use_container_width=True)
        with comp_c4:
            comp_1y = st.button("1Y", key="comp_1y", use_container_width=True, type="primary")
        with comp_c5:
            comp_all = st.button("All", key="comp_all", use_container_width=True)

        if 'comp_period' not in st.session_state:
            st.session_state.comp_period = 365
        if comp_1m:
            st.session_state.comp_period = 30
        elif comp_3m:
            st.session_state.comp_period = 90
        elif comp_6m:
            st.session_state.comp_period = 180
        elif comp_1y:
            st.session_state.comp_period = 365
        elif comp_all:
            st.session_state.comp_period = None

        st.divider()

        # ── Custom Date Range ────────────────────────────────────────────
        custom_c1, custom_c2, custom_c3 = st.columns([2, 2, 1])
        with custom_c1:
            custom_start = st.date_input(
                "Custom Start Date",
                value=pd.Timestamp.now() - pd.Timedelta(days=365),
                max_value=pd.Timestamp.now(),
                help="Select custom start date"
            )
        with custom_c2:
            custom_end = st.date_input(
                "Custom End Date", 
                value=pd.Timestamp.now(),
                max_value=pd.Timestamp.now(),
                help="Select custom end date"
            )
        with custom_c3:
            st.write("") # Spacing
            st.write("")
            if st.button("Apply Custom Range", use_container_width=True, type="secondary"):
                st.session_state.comp_period = "custom"
                st.session_state.custom_start_date = custom_start
                st.session_state.custom_end_date = custom_end
                st.rerun()

        st.divider()

        comp_period_labels = {30: "1M", 90: "3M", 180: "6M", 365: "1Y", None: "All Time"}

        # ── Normalized Price Chart ───────────────────────────────────────
        st.markdown("**📊 Price Performance Comparison**")

        if 'custom_start_date' in st.session_state and 'custom_end_date' in st.session_state and st.session_state.comp_period == "custom":
            start_date = st.session_state.custom_start_date
            end_date = st.session_state.custom_end_date
            period_label = f"Custom: {start_date} to {end_date}"
        elif st.session_state.get('comp_period'):
            end_date = pd.Timestamp.now().date()
            start_date = end_date - pd.Timedelta(days=st.session_state.comp_period)
            period_label = comp_period_labels.get(st.session_state.comp_period, "Custom")
        else:
            start_date = pd.Timestamp('1980-01-01').date()
            end_date = pd.Timestamp.now().date()
            period_label = "All Time"
            
        st.markdown(f"**Normalized to 100 ({period_label})**")

        all_price_data = []
        data_sources = {}

        with st.spinner(f"Loading data for {', '.join(compare_tickers)}..."):
            progress_text = st.empty()
            
            for i, ticker in enumerate(compare_tickers):
                progress_text.text(f"Loading {ticker}... ({i+1}/{len(compare_tickers)})")
                
                ticker_data, source = fetch_comparison_data_smart(
                    ticker, 
                    start_date, 
                    end_date, 
                    fmp_api_key
                )
                
                if not ticker_data.empty:
                    first_price = float(ticker_data.iloc[0]['CLOSE'])
                    if first_price > 0:
                        ticker_data['NORMALIZED'] = (ticker_data['CLOSE'].astype(float) / first_price) * 100
                    
                    all_price_data.append(ticker_data)
                    data_sources[ticker] = source
                else:
                    data_sources[ticker] = "not_found"
            
            progress_text.empty()

        # Show data sources
        st.markdown("**📊 Data Sources:**")
        source_cols = st.columns(len(compare_tickers))

        source_icons = {
            'database': '💾 DB',
            'yahoo_api': '🟡 Yahoo',
            'fmp_api': '🔴 FMP',
            'not_found': '❌ None'
        }

        for i, ticker in enumerate(compare_tickers):
            with source_cols[i]:
                source = data_sources.get(ticker, 'not_found')
                icon = source_icons[source]
                
                if source == 'not_found':
                    st.error(f"{ticker}: {icon}")
                elif source == 'database':
                    st.success(f"{ticker}: {icon}")
                else:
                    st.info(f"{ticker}: {icon}")

        st.divider()

        # Data Coverage Summary
        st.markdown("**📊 Data Coverage Details:**")
        
        coverage_data = []
        for ticker in compare_tickers:
            source = data_sources.get(ticker, 'not_found')
            
            if source != 'not_found':
                ticker_df = next((df for df in all_price_data if df['TICKER'].iloc[0] == ticker), None)
                
                if ticker_df is not None:
                    coverage_data.append({
                        'Stock': ticker,
                        'Source': source_icons[source],
                        'Start Date': ticker_df['DATE'].min().strftime('%Y-%m-%d'),
                        'End Date': ticker_df['DATE'].max().strftime('%Y-%m-%d'),
                        'Data Points': len(ticker_df)
                    })

        if coverage_data:
            coverage_df = pd.DataFrame(coverage_data)
            st.dataframe(coverage_df, use_container_width=True, hide_index=True)

        # Prepare download data (only if we have comparison data)
        if all_price_data:
            # Combine all stock data
            combined_for_download = pd.concat(all_price_data)
            
            # Format for CSV
            download_df = combined_for_download.copy()
            download_df['Date'] = download_df['DATE'].dt.strftime('%Y-%m-%d')
            download_df = download_df[['Date', 'TICKER', 'CLOSE', 'NORMALIZED']]
            download_df = download_df.rename(columns={
                'Date': 'Date',
                'TICKER': 'Stock',
                'CLOSE': 'Price',
                'NORMALIZED': 'Normalized (Base 100)'
            })
            
            csv = download_df.to_csv(index=False)
            
            st.download_button(
                label="📥 Download Comparison Data (CSV)",
                data=csv,
                file_name=f"comparison_{'_'.join(compare_tickers)}_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True,
                type="secondary"
            )

        st.divider()

        if all_price_data:
            combined_prices = pd.concat(all_price_data)

            fig = px.line(
                combined_prices,
                x='DATE',
                y='NORMALIZED',
                color='TICKER',
                labels={'NORMALIZED': 'Normalized Price (Base 100)', 'DATE': 'Date'},
            )

            fig.update_layout(
                paper_bgcolor=T['plotly_paper'],
                plot_bgcolor=T['plotly_plot'],
                font=dict(family='DM Sans', color=T['plotly_text'], size=12),
                xaxis=dict(
                    gridcolor=T['plotly_grid'],
                    showgrid=True,
                    title="Date"
                ),
                yaxis=dict(
                    gridcolor=T['plotly_grid'],
                    showgrid=True,
                    title="Normalized Price (Base 100)",
                    zeroline=True,
                    zerolinecolor=T['border']
                ),
                hovermode='x unified',
                height=500,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1,
                    bgcolor=T['bg_card'],
                    bordercolor=T['border'],
                    borderwidth=1
                )
            )

            # Make lines thicker and more visible
            fig.update_traces(
                line=dict(width=2.5),
                marker=dict(size=3)
            )

            # Add a horizontal reference line at 100
            fig.add_hline(
                y=100, 
                line_dash="dash", 
                line_color=T['text_muted'],
                annotation_text="Starting Point (100)",
                annotation_position="right"
            )

            st.plotly_chart(fig, use_container_width=True)

            actual_start = combined_prices['DATE'].min()
            actual_end = combined_prices['DATE'].max()
            actual_days = (actual_end - actual_start).days
            
            expected_label = comp_period_labels.get(st.session_state.comp_period, "Custom")
            
            st.caption(f"""
            **📊 Showing {len(combined_prices)} data points**<br>
            📅 {actual_start.strftime('%b %d, %Y')} → {actual_end.strftime('%b %d, %Y')} ({actual_days} days)<br>
            🎯 Selected period: {expected_label}
            """, unsafe_allow_html=True)
            
            # Warning if mismatch
            if st.session_state.comp_period != "custom" and st.session_state.comp_period and abs(actual_days - st.session_state.comp_period) > 30:
                st.warning(f"⚠️ Expected ~{st.session_state.comp_period} days, got {actual_days} days. May indicate data gaps.")

        else:
            st.error(f"""
            ❌ Could not load data for any stocks in the selected period.
            
            **Selected Range:** {start_date} → {end_date}
            
            **Suggestions:**
            - Try a different date range
            - Check if ticker symbols are correct
            - Verify API keys are configured
            """)

        st.divider()
        st.markdown("**📊 Performance Statistics**")

        stats_cols = st.columns(len(compare_tickers))

        for i, ticker in enumerate(compare_tickers):
            ticker_df = next((df for df in all_price_data if df['TICKER'].iloc[0] == ticker), None)
            
            if ticker_df is not None:
                with stats_cols[i]:
                    start_price = ticker_df.iloc[0]['CLOSE']
                    end_price = ticker_df.iloc[-1]['CLOSE']
                    total_return = ((end_price - start_price) / start_price) * 100
                    
                    max_price = ticker_df['CLOSE'].max()
                    min_price = ticker_df['CLOSE'].min()
                    volatility = ticker_df['CLOSE'].std()
                    
                    st.markdown(f"**{ticker}**")
                    st.metric("Total Return", f"{total_return:+.2f}%")
                    st.caption(f"High: ${max_price:.2f}")
                    st.caption(f"Low: ${min_price:.2f}")
                    st.caption(f"Volatility: ${volatility:.2f}")

        st.divider()

        # ── Statistics Comparison Table ───────────────────────────────
        st.markdown("**📊 Statistics Comparison**")
        stats_list = []
        for ticker in compare_tickers:
            try:
                tstats = run_query(f"""
                    SELECT
                        '{ticker}' as ticker,
                        MIN(low) as all_time_low,
                        MAX(high) as all_time_high,
                        AVG(close) as avg_price,
                        AVG(volume) as avg_volume
                    FROM FINANCE_AI_DB.STOCK_DATA.PRICES
                    WHERE ticker = '{ticker}'
                """)
                if tstats is not None and not tstats.empty:
                    stats_list.append(tstats.iloc[0])
            except Exception:
                pass

        if stats_list:
            stats_df = pd.DataFrame(stats_list)
            display_df = pd.DataFrame({
                'Stock': stats_df['TICKER'],
                'All-Time High': stats_df['ALL_TIME_HIGH'].apply(lambda x: f"${x:.2f}"),
                'All-Time Low': stats_df['ALL_TIME_LOW'].apply(lambda x: f"${x:.2f}"),
                'Avg Price': stats_df['AVG_PRICE'].apply(lambda x: f"${x:.2f}"),
                'Avg Volume': stats_df['AVG_VOLUME'].apply(lambda x: f"{x:,.0f}")
            })
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.caption("Statistics unavailable")

        # Download button
        download_df = comp_df.copy()
        download_df['Date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        csv = download_df.to_csv(index=False)
        col_dl1, col_dl2 = st.columns([1, 4])
        with col_dl1:
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"stock_comparison_{'_'.join(compare_tickers)}_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )

        st.divider()

        # ── Comparison Chatbot ─────────────────────────────────────────
        st.markdown("**💬 Ask Questions About These Stocks**")
        st.caption(f"Chat with AI to compare {', '.join(compare_tickers)}. Ask about performance differences, which to buy, risks, sector trends, etc.")

        with st.expander("💡 Try asking...", expanded=False):
            st.markdown("""
            **Comparison:**
            - "Which stock has the best growth potential?"
            - "Which is more volatile, and why?"
            - "Which stock is cheaper based on fundamentals?"

            **Investment:**
            - "If I can only buy one, which should it be and why?"
            - "What's the biggest risk for each stock?"
            - "Which has better momentum right now?"

            **Strategy:**
            - "Should I hold all of these or concentrate on one?"
            - "How do these stocks correlate with each other?"
            """)

        comparison_key = "_".join(sorted(compare_tickers))
        comp_chat_key = f"comparison_chat_{comparison_key}"
        if comp_chat_key not in st.session_state:
            st.session_state[comp_chat_key] = []

        # Clear chat button
        if st.session_state.get(comp_chat_key):
            _ce, _cc = st.columns([5, 1])
            with _cc:
                if st.button("🗑️ Clear", key=f"clear_comp_{comparison_key}", use_container_width=True):
                    st.session_state[comp_chat_key] = []
                    st.rerun()

        for msg in st.session_state[comp_chat_key]:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        if user_question := st.chat_input(f"Compare {', '.join(compare_tickers)}..."):
            st.session_state[comp_chat_key].append({"role": "user", "content": user_question})
            with st.chat_message("user"):
                st.markdown(user_question)

            with st.chat_message("assistant"):
                with st.spinner("Analyzing..."):
                    chat_context = build_comparison_chat_context(compare_tickers, comp_df, route_results)
                    chat_prompt = f"""You are a senior financial analyst helping compare multiple stocks.

{chat_context}

User Question: {user_question}

Provide a clear, insightful answer (3-4 sentences) that:
1. Directly addresses the question
2. Uses specific numbers from the data
3. Compares the stocks against each other
4. Gives actionable insights

Be professional but conversational. If data is missing, acknowledge it and provide general insights."""

                    response, model = call_llm(chat_prompt, "comparison_chat")
                    badge = "⚡ GROQ" if model == "groq" else "🛡️ CORTEX"
                    st.markdown(f"**{badge}**")
                    st.markdown(response)
                    st.session_state[comp_chat_key].append({
                        "role": "assistant",
                        "content": f"**{badge}**\n\n{response}"
                    })

    else:
        # ── Default single-stock view ────────────────────────────────────
        st.markdown(f"""
        <div class="section-header">
            <span class="section-title">{dashboard_ticker} Intelligence Report</span>
            <div class="section-line"></div>
            <span class="gold-tag">Deep Analysis</span>
        </div>
        """, unsafe_allow_html=True)

        route_info = smart_data_router([dashboard_ticker], fmp_api_key).get(dashboard_ticker, {})

        if route_info.get("source") == "not_found":
            st.error(f"Stock {dashboard_ticker} not found in database or API.")
            st.stop()

        if route_info.get("api_data") is not None and len(route_info["api_data"]) > 0:
            latest = route_info["api_data"].iloc[-1]
        else:
            try:
                db_latest = run_query(f"""
                    SELECT * FROM FINANCE_AI_DB.STOCK_DATA.PRICES
                    WHERE ticker = '{dashboard_ticker.upper()}'
                    ORDER BY date DESC LIMIT 1
                """)
                if db_latest is not None and len(db_latest) > 0:
                    latest = db_latest.iloc[0]
                else:
                    st.warning("No price data available.")
                    st.stop()
            except Exception as e:
                st.error(f"Error fetching data: {e}")
                st.stop()

        # ── Metrics Row ──────────────────────────────────────────────────
        m1, m2, m3, m4 = st.columns(4)
        day_change = float(latest['CLOSE']) - float(latest['OPEN'])
        day_change_pct = (day_change / float(latest['OPEN'])) * 100 if float(latest['OPEN']) != 0 else 0

        with m1:
            st.metric("Current Price", f"${latest['CLOSE']:.2f}")
        with m2:
            st.metric("Day Change", f"${abs(day_change):.2f}", delta=f"{day_change_pct:+.2f}%")
        with m3:
            st.metric("Day High", f"${latest['HIGH']:.2f}")
        with m4:
            st.metric("Volume", f"{latest['VOLUME']:,.0f}")

        st.divider()

        # ── Timeline Selector ────────────────────────────────────────────
        st.markdown("**📅 Select Time Period**")
        timeline_cols = st.columns(5)
        with timeline_cols[0]:
            btn_1w = st.button("1 Week", use_container_width=True, key="single_1w")
        with timeline_cols[1]:
            btn_1m = st.button("1 Month", use_container_width=True, key="single_1m")
        with timeline_cols[2]:
            btn_6m = st.button("6 Months", use_container_width=True, key="single_6m")
        with timeline_cols[3]:
            btn_1y = st.button("1 Year", use_container_width=True, type="primary", key="single_1y")
        with timeline_cols[4]:
            btn_all = st.button("All Time", use_container_width=True, key="single_all")

        if 'single_period' not in st.session_state:
            st.session_state.single_period = 365
        if btn_1w:
            st.session_state.single_period = 7
        elif btn_1m:
            st.session_state.single_period = 30
        elif btn_6m:
            st.session_state.single_period = 180
        elif btn_1y:
            st.session_state.single_period = 365
        elif btn_all:
            st.session_state.single_period = None

        period_labels = {7: "1 Week", 30: "1 Month", 180: "6 Months", 365: "1 Year", None: "All Time"}
        current_label = period_labels.get(st.session_state.single_period, "1 Year")

        st.divider()

        # ── Candlestick Chart ────────────────────────────────────────────
        st.markdown(f"**📈 Price Chart — {current_label}**")
        
        # Determine date range for single stock
        if st.session_state.get('single_period'):
            end_date = pd.Timestamp.now().date()
            start_date = end_date - pd.Timedelta(days=st.session_state.single_period)
        else:
            start_date = pd.Timestamp('1980-01-01').date()
            end_date = pd.Timestamp.now().date()

        # Use smart fetcher for single stock too
        single_data, single_source = fetch_comparison_data_smart(
            dashboard_ticker,
            start_date,
            end_date,
            fmp_api_key
        )

        if not single_data.empty:
            # Show data source badge
            source_badge = {
                'database': '💾 Historical Database',
                'yahoo_api': '🟡 Yahoo Finance (Real-time)',
                'fmp_api': '🔴 FMP API (Real-time)',
            }
            
            st.caption(f"Data Source: {source_badge.get(single_source, 'Unknown')}")
            
            # Update route_info with fetched data source
            route_info['api_data'] = single_data
            route_info['source'] = single_source
            
            # Now call chart function
            show_stock_chart(dashboard_ticker, st.session_state.single_period, T, route_info)
        else:
            st.error(f"❌ Could not load data for {dashboard_ticker} in selected period")

        st.divider()

        # ── Statistics Section ───────────────────────────────────────────
        stat_col1, stat_col2 = st.columns(2)

        with stat_col1:
            st.markdown("**📊 Historical Statistics**")
            try:
                stats = run_query(f"""
                    SELECT
                        MIN(low)    as all_time_low,
                        MAX(high)   as all_time_high,
                        AVG(close)  as avg_price,
                        AVG(volume) as avg_volume
                    FROM FINANCE_AI_DB.STOCK_DATA.PRICES
                    WHERE ticker = '{dashboard_ticker}'
                """)
                stats_row = stats.iloc[0]
                st.markdown(f"""
                <table style="width:100%; color:{T['text_primary']}; font-family:'DM Sans', sans-serif;">
                    <tr style="border-bottom: 1px solid {T['border']};">
                        <td style="padding: 8px 0;">All-Time High</td>
                        <td style="text-align:right; font-weight:600;">${stats_row['ALL_TIME_HIGH']:.2f}</td>
                    </tr>
                    <tr style="border-bottom: 1px solid {T['border']};">
                        <td style="padding: 8px 0;">All-Time Low</td>
                        <td style="text-align:right; font-weight:600;">${stats_row['ALL_TIME_LOW']:.2f}</td>
                    </tr>
                    <tr style="border-bottom: 1px solid {T['border']};">
                        <td style="padding: 8px 0;">Average Price</td>
                        <td style="text-align:right;">${stats_row['AVG_PRICE']:.2f}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px 0;">Avg Daily Volume</td>
                        <td style="text-align:right;">{stats_row['AVG_VOLUME']:,.0f}</td>
                    </tr>
                </table>
                """, unsafe_allow_html=True)
            except Exception:
                st.caption("Historical stats unavailable")

        with stat_col2:
            st.markdown("**🎯 Performance Metrics**")
            try:
                perf = run_query(f"""
                    WITH prices AS (
                        SELECT date, close,
                               LAG(close, 365) OVER (ORDER BY date) as close_1y_ago,
                               LAG(close, 30)  OVER (ORDER BY date) as close_1m_ago
                        FROM FINANCE_AI_DB.STOCK_DATA.PRICES
                        WHERE ticker = '{dashboard_ticker}'
                    )
                    SELECT
                        ((close - close_1y_ago) / close_1y_ago * 100) as return_1y,
                        ((close - close_1m_ago) / close_1m_ago * 100) as return_1m
                    FROM prices
                    WHERE close_1y_ago IS NOT NULL
                    ORDER BY date DESC
                    LIMIT 1
                """)
                if not perf.empty:
                    perf_row = perf.iloc[0]
                    r1y_color = T['green'] if perf_row['RETURN_1Y'] >= 0 else T['red']
                    r1m_color = T['green'] if perf_row['RETURN_1M'] >= 0 else T['red']
                    st.markdown(f"""
                    <table style="width:100%; color:{T['text_primary']}; font-family:'DM Sans', sans-serif;">
                        <tr style="border-bottom: 1px solid {T['border']};">
                            <td style="padding: 8px 0;">1-Month Return</td>
                            <td style="text-align:right; font-weight:700; color:{r1m_color};">
                                {perf_row['RETURN_1M']:+.2f}%
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 8px 0;">1-Year Return</td>
                            <td style="text-align:right; font-weight:700; color:{r1y_color};">
                                {perf_row['RETURN_1Y']:+.2f}%
                            </td>
                        </tr>
                    </table>
                    """, unsafe_allow_html=True)
                else:
                    st.caption("Not enough data for performance metrics")
            except Exception:
                st.caption("Performance metrics unavailable")

        st.divider()

        # ── AI Research Report ───────────────────────────────────────────
        st.markdown("**🤖 AI Research Report**")
        with st.spinner("Generating deep analysis..."):
            try:
                research_data = run_query(f"""
                    SELECT YEAR(date) as year,
                           AVG(close) as avg_price,
                           MAX(high) as year_high,
                           MIN(low) as year_low
                    FROM FINANCE_AI_DB.STOCK_DATA.PRICES
                    WHERE ticker = '{dashboard_ticker}'
                    GROUP BY YEAR(date)
                    ORDER BY year DESC
                    LIMIT 5
                """)

                research_prompt = f"""You are a senior equity research analyst at JPMorgan.

Write a comprehensive research report on {dashboard_ticker}.

Latest Price: ${latest['CLOSE']:.2f}
Historical Performance (last 5 years):
{research_data.to_string()}

Structure your report with these sections:

**Executive Summary** (2 sentences: current status, key trend)

**Historical Analysis** (3 sentences: performance patterns over 5 years, volatility assessment, notable events)

**Investment Outlook** (2 sentences: forward-looking view, risk/reward balance)

Be specific with numbers from the data. Use professional Wall Street analyst tone.
Format with markdown bold for section headers."""

                report, model = call_llm(research_prompt, "research")
                badge = "⚡ GROQ" if model == "groq" else "🛡️ CORTEX"

                st.markdown(f"""
                <div class="insight-box">
                    <div class="insight-label">⬡ Research Report — {badge} Intelligence</div>
                    {report}
                </div>
                """, unsafe_allow_html=True)
            except Exception as e:
                st.caption(f"AI report unavailable: {e}")

        st.divider()

        # ── AI Chatbot ─────────────────────────────────────────────────
        st.markdown("**💬 Ask Questions About This Stock**")
        st.caption(f"Chat with AI to learn more about {dashboard_ticker}. Ask about performance, risks, comparison with competitors, etc.")

        with st.expander("💡 Try asking...", expanded=False):
            st.markdown(f"""
            **Performance:**
            - "How has {dashboard_ticker} performed compared to the market?"
            - "Is {dashboard_ticker} overvalued or undervalued right now?"
            - "What's the main catalyst for {dashboard_ticker}'s recent move?"

            **Investment:**
            - "Should I buy {dashboard_ticker} at the current price?"
            - "What are the top 3 risks for {dashboard_ticker}?"
            - "Compare {dashboard_ticker} to its main competitors"

            **Technical:**
            - "What's the support and resistance levels for {dashboard_ticker}?"
            - "Is {dashboard_ticker} showing bullish or bearish momentum?"
            """)

        chat_key = f"chat_history_{dashboard_ticker}"
        if chat_key not in st.session_state:
            st.session_state[chat_key] = []

        # Clear chat button
        if st.session_state.get(chat_key):
            _se, _sc = st.columns([5, 1])
            with _sc:
                if st.button("🗑️ Clear", key=f"clear_single_{dashboard_ticker}", use_container_width=True):
                    st.session_state[chat_key] = []
                    st.rerun()

        for msg in st.session_state[chat_key]:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"], unsafe_allow_html=True)
                
                # Show SQL query if available
                if msg.get("sql"):
                    with st.expander("🔍 Generated SQL"):
                        st.code(msg["sql"], language="sql")
                
                # Show data if available
                if msg.get("data") is not None:
                    df = msg["data"]
                    st.dataframe(df, use_container_width=True)
                    
                    # Show chart if time-series
                    if 'DATE' in df.columns and len(df) > 1:
                        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
                        chart_y = 'CLOSE' if 'CLOSE' in numeric_cols else (numeric_cols[0] if numeric_cols else None)
                        
                        if chart_y:
                            try:
                                fig = px.line(df, x='DATE', y=chart_y)
                                fig.update_layout(
                                    paper_bgcolor=T['plotly_paper'],
                                    plot_bgcolor=T['plotly_plot'],
                                    font=dict(color=T['plotly_text']),
                                    height=300
                                )
                                st.plotly_chart(fig, use_container_width=True)
                            except:
                                pass
                    
                    # Download button
                    csv = df.to_csv(index=False)
                    st.download_button(
                        "📥 Download",
                        csv,
                        "data.csv",
                        "text/csv",
                        key=f"dl_hist_{msg.get('id', id(msg))}"
                    )

        if user_question := st.chat_input(f"Ask about {dashboard_ticker}..."):
            st.session_state[chat_key].append({"role": "user", "content": user_question})
            with st.chat_message("user"):
                st.markdown(user_question)

            with st.chat_message("assistant"):
                with st.spinner("Analyzing..."):
                    # Build comprehensive context
                    chat_context = build_single_stock_chat_context(dashboard_ticker, latest, route_info)
                    
                    # Check if user wants data/chart
                    needs_data = any(word in user_question.lower() for word in [
                        'show', 'data', 'chart', 'graph', 'trend', 'performance', 
                        'history', 'compare', 'analysis', 'calculate', 'return'
                    ])
                    
                    if needs_data:
                        # Generate SQL query to fetch data
                        sql_prompt = f"""You are a SQL expert for stock analysis.
Database: FINANCE_AI_DB.STOCK_DATA.PRICES
Columns: date, ticker, open, high, low, close, volume
Current stock: {dashboard_ticker}

User question: {user_question}

Generate a Snowflake SQL query to answer this question.
- For price history: SELECT date, close FROM ... WHERE ticker = '{dashboard_ticker}' ORDER BY date LIMIT 365
- For performance: calculate percentage changes using LAG() or MIN/MAX
- Always include date column for time-series
- LIMIT to 500 rows maximum

Return ONLY the SQL query, no markdown, no explanations."""

                        sql_query, _ = call_llm(sql_prompt, "sql_generation")
                        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
                        
                        if ';' in sql_query:
                            sql_query = sql_query.split(';')[0].strip()
                        
                        # Execute query
                        try:
                            result_df = run_query(sql_query)
                            
                            if not result_df.empty:
                                # Show SQL in expander
                                with st.expander("🔍 Generated SQL"):
                                    st.code(sql_query, language="sql")
                                
                                # Display data
                                st.dataframe(result_df, use_container_width=True)
                                
                                # Auto-generate chart if time-series data
                                if 'DATE' in result_df.columns and len(result_df) > 1:
                                    numeric_cols = result_df.select_dtypes(include=['float64', 'int64']).columns.tolist()
                                    chart_y = 'CLOSE' if 'CLOSE' in numeric_cols else (numeric_cols[0] if numeric_cols else None)
                                    
                                    if chart_y:
                                        try:
                                            fig = px.line(
                                                result_df,
                                                x='DATE',
                                                y=chart_y,
                                                title=f"{dashboard_ticker} - {chart_y.title()} Over Time"
                                            )
                                            fig.update_layout(
                                                paper_bgcolor=T['plotly_paper'],
                                                plot_bgcolor=T['plotly_plot'],
                                                font=dict(color=T['plotly_text']),
                                                xaxis=dict(gridcolor=T['plotly_grid']),
                                                yaxis=dict(gridcolor=T['plotly_grid']),
                                                height=400
                                            )
                                            st.plotly_chart(fig, use_container_width=True)
                                        except:
                                            pass
                                
                                # Generate AI analysis based on data
                                data_summary = result_df.head(20).to_string()
                                analysis_prompt = f"""You are a senior financial analyst at JPMorgan.

Stock: {dashboard_ticker}
Current Price: ${latest['CLOSE']:.2f}

User asked: {user_question}

Data retrieved:
{data_summary}

Provide a professional 3-4 sentence analysis that:
1. Directly answers the question
2. References specific numbers from the data
3. Provides investment context
4. Gives actionable insights

Be specific with numbers. Use professional Wall Street analyst tone."""

                                analysis, model = call_llm(analysis_prompt, "analysis")
                                badge = "⚡ GROQ" if model == "groq" else "🛡️ CORTEX"
                                
                                # Sentiment analysis
                                ins_lower = analysis.lower()
                                bull_words = ["up", "gains", "positive", "outperforming", "bullish", "higher", "surge"]
                                bear_words = ["down", "decline", "negative", "underperforming", "bearish", "lower", "drop"]
                                bull_count = sum(1 for w in bull_words if w in ins_lower)
                                bear_count = sum(1 for w in bear_words if w in ins_lower)
                                
                                if bull_count > bear_count:
                                    sentiment_html = '<span style="background-color:rgba(46,125,50,0.2);color:#81c784;border:1px solid #2e7d32;padding:2px 8px;border-radius:12px;font-size:0.8em;margin-left:5px;">🟢 BULLISH</span>'
                                elif bear_count > bull_count:
                                    sentiment_html = '<span style="background-color:rgba(198,40,40,0.2);color:#e57373;border:1px solid #c62828;padding:2px 8px;border-radius:12px;font-size:0.8em;margin-left:5px;">🔴 BEARISH</span>'
                                else:
                                    sentiment_html = '<span style="background-color:rgba(249,168,37,0.2);color:#fff59d;border:1px solid #f9a825;padding:2px 8px;border-radius:12px;font-size:0.8em;margin-left:5px;">🟡 NEUTRAL</span>'
                                
                                st.markdown(f"""
                                {sentiment_html}
                                <div class="insight-box">
                                    <div class="insight-label">⬡ AI Analysis — {badge} Intelligence</div>
                                    {analysis}
                                </div>
                                """, unsafe_allow_html=True)
                                
                                # Download button
                                csv = result_df.to_csv(index=False)
                                st.download_button(
                                    "📥 Download Data",
                                    csv,
                                    f"{dashboard_ticker}_data.csv",
                                    "text/csv",
                                    key=f"dl_single_{len(st.session_state[chat_key])}"
                                )
                                
                                # Save to chat history
                                st.session_state[chat_key].append({
                                    "role": "assistant",
                                    "content": f"{sentiment_html}<br>{analysis}",
                                    "sql": sql_query,
                                    "data": result_df
                                })
                                
                        except Exception as e:
                            # Fallback to simple chat if SQL fails
                            chat_prompt = f"""You are an expert financial analyst assistant specialized in stock analysis.

{chat_context}

User Question: {user_question}

Provide a comprehensive answer (3-4 sentences) that:
1. Directly answers the question with specifics
2. References actual data points and numbers
3. Provides context (historical perspective, sector trends, comparisons)
4. Gives actionable insights or recommendations when relevant

Be professional, accurate, and helpful. If you need data not provided, acknowledge it and give the best general guidance."""

                            response, model = call_llm(chat_prompt, "chat")
                            badge = "⚡ GROQ" if model == "groq" else "🛡️ CORTEX"

                            st.markdown(f"**{badge}**")
                            st.markdown(response)

                            st.session_state[chat_key].append({
                                "role": "assistant",
                                "content": f"**{badge}**\n\n{response}"
                            })
                    else:
                        # Simple conversational response (no data needed)
                        chat_prompt = f"""You are an expert financial analyst assistant specialized in stock analysis.

{chat_context}

User Question: {user_question}

Provide a comprehensive answer (3-4 sentences) that:
1. Directly answers the question with specifics
2. References actual data points and numbers from the context
3. Provides context (historical perspective, sector trends, comparisons)
4. Gives actionable insights or recommendations when relevant

Be professional, accurate, and helpful."""

                        response, model = call_llm(chat_prompt, "chat")
                        badge = "⚡ GROQ" if model == "groq" else "🛡️ CORTEX"

                        st.markdown(f"**{badge}**")
                        st.markdown(response)

                        st.session_state[chat_key].append({
                            "role": "assistant",
                            "content": f"**{badge}**\n\n{response}"
                        })

    st.stop()

# If current_page == "📊 Analysis" just continue with the rest of the existing code below normally

# ─── DATASET METRICS ─────────────────────────────────────────────────────────
try:
    stats = run_query("""
        SELECT COUNT(*) as TOTAL_RECORDS, COUNT(DISTINCT ticker) as TOTAL_STOCKS,
               MIN(date) as EARLIEST_DATE, MAX(date) as LATEST_DATE
        FROM FINANCE_AI_DB.STOCK_DATA.PRICES
    """)
    row = stats.iloc[0]
    total_rec = f"{int(row['TOTAL_RECORDS']):,}"
    total_stocks = f"{int(row['TOTAL_STOCKS']):,}"
    date_range = f"{pd.to_datetime(row['EARLIEST_DATE']).year}–{pd.to_datetime(row['LATEST_DATE']).year}"
except:
    total_rec = "29,677,722"
    total_stocks = "7,693"
    date_range = "1962–2023"

st.markdown(f"""
<div class="metrics-row">
    <div class="metric-cell">
        <div class="metric-label">Total Records</div>
        <div class="metric-value">{total_rec}</div>
        <div class="metric-sub">Price data points</div>
    </div>
    <div class="metric-cell">
        <div class="metric-label">Unique Tickers</div>
        <div class="metric-value">{total_stocks}</div>
        <div class="metric-sub">NYSE · NASDAQ · NYSE</div>
    </div>
    <div class="metric-cell">
        <div class="metric-label">Coverage</div>
        <div class="metric-value" style="font-size:1.3rem;">{date_range}</div>
        <div class="metric-sub">62 years of market data</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ─── CONTROLS ROW ─────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="section-header">
    <span class="section-title">Market Controls</span>
    <div class="section-line"></div>
    <span class="gold-tag">Filters</span>
</div>
""", unsafe_allow_html=True)

ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 2, 2])

with ctrl_col1:
    selected_tickers = st.multiselect(
        "Compare Stocks",
        options=[
            # Big Tech
            "AAPL","MSFT","GOOGL","GOOG","AMZN","META","NVDA","TSLA","NFLX","AMD",
            # Finance
            "JPM","BAC","GS","MS","WFC","C","BLK","AXP","V","MA","PYPL","SQ",
            # Healthcare
            "JNJ","PFE","UNH","ABBV","MRK","LLY","BMY","AMGN","GILD","CVS",
            # Energy
            "XOM","CVX","COP","SLB","EOG","MPC","PSX","VLO","OXY","HAL",
            # Consumer
            "WMT","TGT","COST","HD","LOW","NKE","SBUX","MCD","YUM",
            # Industrial
            "BA","CAT","GE","HON","MMM","LMT","RTX","NOC","DE","EMR",
            # Telecom & Media
            "T","VZ","CMCSA","DIS","PARA","WBD","SPOT","SNAP","TWTR",
            # Semiconductors
            "INTC","QCOM","TXN","MU","AMAT","LRCX","KLAC","MRVL","AVGO","ARM",
            # Cloud & Software
            "CRM","ORCL","SAP","ADBE","NOW","SNOW","PLTR","DDOG","ZS","CRWD",
            # ETFs & Indices
            "SPY","QQQ","DIA","IWM","VTI","GLD","SLV","USO","TLT","HYG",
            # Other Notable
            "BRK.B","IBM","UBER","LYFT","ABNB","DASH","RBLX","U","RIVN","LCID"
        ],
        default=["AAPL", "MSFT"],
        help="Select multiple tickers to compare"
    )

with ctrl_col2:
    date_from = st.date_input("From Date", value=date(2020, 1, 1),
                              min_value=date(1962, 1, 1), max_value=date(2023, 12, 31))

with ctrl_col3:
    date_to = st.date_input("To Date", value=date(2023, 12, 31),
                            min_value=date(1962, 1, 1), max_value=date(2023, 12, 31))

# ─── CHARTS SECTION ──────────────────────────────────────────────────────────
if selected_tickers:
    st.markdown(f"""
    <div class="section-header">
        <span class="section-title">Price Analysis</span>
        <div class="section-line"></div>
        <span class="gold-tag">Interactive</span>
    </div>
    """, unsafe_allow_html=True)

    tickers_str = ", ".join([f"'{t}'" for t in selected_tickers])
    chart_tab1, chart_tab2, chart_tab3 = st.tabs(["📈 Price Trend", "🕯️ Candlestick", "📊 Volume"])

    # ── Price Trend Tab ──
    with chart_tab1:
        try:
            price_data = run_query(f"""
                SELECT date, ticker, close
                FROM FINANCE_AI_DB.STOCK_DATA.PRICES
                WHERE ticker IN ({tickers_str})
                  AND date BETWEEN '{date_from}' AND '{date_to}'
                ORDER BY date
            """)
            price_data['DATE'] = pd.to_datetime(price_data['DATE'])

            fig = go.Figure()
            colors = [T['gold'], '#4A90D9', '#E74C3C', '#2ECC71', '#9B59B6',
                      '#F39C12', '#1ABC9C', '#E67E22']

            for i, ticker in enumerate(selected_tickers):
                df_t = price_data[price_data['TICKER'] == ticker]
                if len(df_t) > 0:
                    fig.add_trace(go.Scatter(
                        x=df_t['DATE'], y=df_t['CLOSE'],
                        name=ticker, mode='lines',
                        line=dict(color=colors[i % len(colors)], width=1.8),
                        hovertemplate=f"<b>{ticker}</b><br>%{{x|%b %d, %Y}}<br>$%{{y:.2f}}<extra></extra>"
                    ))

            fig.update_layout(
                paper_bgcolor=T['plotly_paper'], plot_bgcolor=T['plotly_plot'],
                font=dict(family='DM Sans', color=T['plotly_text'], size=11),
                legend=dict(bgcolor='rgba(0,0,0,0)', bordercolor=T['border'],
                           borderwidth=1, font=dict(size=11)),
                xaxis=dict(gridcolor=T['plotly_grid'], showgrid=True, gridwidth=0.5,
                           tickfont=dict(family='JetBrains Mono', size=10),
                           zeroline=False, showspikes=True, spikecolor=T['gold'],
                           spikethickness=1, spikedash='dot'),
                yaxis=dict(gridcolor=T['plotly_grid'], showgrid=True, gridwidth=0.5,
                           tickprefix='$', tickfont=dict(family='JetBrains Mono', size=10),
                           zeroline=False, showspikes=True, spikecolor=T['gold'],
                           spikethickness=1, spikedash='dot'),
                hovermode='x unified',
                margin=dict(l=10, r=10, t=20, b=10),
                height=420,
            )
            st.plotly_chart(fig, use_container_width=True)

            # Export button
            csv_data = price_data.to_csv(index=False)
            st.download_button("📥 Download Price Data", csv_data, "price_trend.csv", "text/csv")

        except Exception as e:
            pass

    # ── Candlestick Tab ──
    with chart_tab2:
        if len(selected_tickers) > 1:
            candle_ticker = st.selectbox("Select ticker for candlestick", selected_tickers)
        else:
            candle_ticker = selected_tickers[0]

        try:
            candle_data = run_query(f"""
                SELECT date, open, high, low, close, volume
                FROM FINANCE_AI_DB.STOCK_DATA.PRICES
                WHERE ticker = '{candle_ticker}'
                  AND date BETWEEN '{date_from}' AND '{date_to}'
                ORDER BY date
            """)
            candle_data['DATE'] = pd.to_datetime(candle_data['DATE'])

            fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                               vertical_spacing=0.04, row_heights=[0.75, 0.25])

            fig.add_trace(go.Candlestick(
                x=candle_data['DATE'],
                open=candle_data['OPEN'], high=candle_data['HIGH'],
                low=candle_data['LOW'], close=candle_data['CLOSE'],
                name=candle_ticker,
                increasing=dict(line=dict(color=T['green'], width=1), fillcolor=T['green']),
                decreasing=dict(line=dict(color=T['red'], width=1), fillcolor=T['red']),
                hovertext=candle_ticker
            ), row=1, col=1)

            # Moving averages
            if len(candle_data) > 20:
                candle_data['MA20'] = candle_data['CLOSE'].rolling(20).mean()
                fig.add_trace(go.Scatter(
                    x=candle_data['DATE'], y=candle_data['MA20'],
                    name='MA 20', line=dict(color=T['gold'], width=1.2, dash='dot'),
                    hovertemplate="MA20: $%{y:.2f}<extra></extra>"
                ), row=1, col=1)

            if len(candle_data) > 50:
                candle_data['MA50'] = candle_data['CLOSE'].rolling(50).mean()
                fig.add_trace(go.Scatter(
                    x=candle_data['DATE'], y=candle_data['MA50'],
                    name='MA 50', line=dict(color='#4A90D9', width=1.2, dash='dot'),
                    hovertemplate="MA50: $%{y:.2f}<extra></extra>"
                ), row=1, col=1)

            # Volume bars
            colors_vol = [T['green'] if c >= o else T['red']
                         for c, o in zip(candle_data['CLOSE'], candle_data['OPEN'])]
            fig.add_trace(go.Bar(
                x=candle_data['DATE'], y=candle_data['VOLUME'],
                name='Volume', marker_color=colors_vol, opacity=0.7,
                hovertemplate="Vol: %{y:,.0f}<extra></extra>"
            ), row=2, col=1)

            fig.update_layout(
                paper_bgcolor=T['plotly_paper'], plot_bgcolor=T['plotly_plot'],
                font=dict(family='DM Sans', color=T['plotly_text'], size=11),
                legend=dict(bgcolor='rgba(0,0,0,0)', bordercolor=T['border'], borderwidth=1),
                xaxis=dict(gridcolor=T['plotly_grid'], rangeslider_visible=False,
                           tickfont=dict(family='JetBrains Mono', size=10)),
                yaxis=dict(gridcolor=T['plotly_grid'], tickprefix='$',
                           tickfont=dict(family='JetBrains Mono', size=10)),
                xaxis2=dict(gridcolor=T['plotly_grid'], tickfont=dict(family='JetBrains Mono', size=10)),
                yaxis2=dict(gridcolor=T['plotly_grid'], tickfont=dict(family='JetBrains Mono', size=9)),
                margin=dict(l=10, r=10, t=20, b=10),
                height=500,
            )
            st.plotly_chart(fig, use_container_width=True)

            csv_candle = candle_data.to_csv(index=False)
            st.download_button("📥 Download OHLCV Data", csv_candle, f"{candle_ticker}_ohlcv.csv", "text/csv")

        except Exception as e:
            pass

    # ── Volume Tab ──
    with chart_tab3:
        try:
            vol_data = run_query(f"""
                SELECT DATE_TRUNC('month', date) as month, ticker, AVG(volume) as avg_volume
                FROM FINANCE_AI_DB.STOCK_DATA.PRICES
                WHERE ticker IN ({tickers_str})
                  AND date BETWEEN '{date_from}' AND '{date_to}'
                GROUP BY 1, 2
                ORDER BY 1
            """)
            vol_data['MONTH'] = pd.to_datetime(vol_data['MONTH'])

            fig = go.Figure()
            colors = [T['gold'], '#4A90D9', '#E74C3C', '#2ECC71', '#9B59B6']

            for i, ticker in enumerate(selected_tickers):
                df_t = vol_data[vol_data['TICKER'] == ticker]
                if len(df_t) > 0:
                    fig.add_trace(go.Bar(
                        x=df_t['MONTH'], y=df_t['AVG_VOLUME'],
                        name=ticker, marker_color=colors[i % len(colors)],
                        opacity=0.85,
                        hovertemplate=f"<b>{ticker}</b><br>%{{x|%b %Y}}<br>Avg Vol: %{{y:,.0f}}<extra></extra>"
                    ))

            fig.update_layout(
                barmode='group',
                paper_bgcolor=T['plotly_paper'], plot_bgcolor=T['plotly_plot'],
                font=dict(family='DM Sans', color=T['plotly_text'], size=11),
                legend=dict(bgcolor='rgba(0,0,0,0)', bordercolor=T['border'], borderwidth=1),
                xaxis=dict(gridcolor=T['plotly_grid'], tickfont=dict(family='JetBrains Mono', size=10)),
                yaxis=dict(gridcolor=T['plotly_grid'], tickfont=dict(family='JetBrains Mono', size=10),
                           title='Avg Monthly Volume'),
                margin=dict(l=10, r=10, t=20, b=10),
                height=420,
            )
            st.plotly_chart(fig, use_container_width=True)

            csv_vol = vol_data.to_csv(index=False)
            st.download_button("📥 Download Volume Data", csv_vol, "volume_data.csv", "text/csv")

        except Exception as e:
            pass

# ─── AI CHAT SECTION ─────────────────────────────────────────────────────────
st.markdown(f"""
<div class="section-header">
    <span class="section-title">AI Research Assistant</span>
    <div class="section-line"></div>
    <span class="gold-tag">Smart AI · Live Data</span>
</div>
""", unsafe_allow_html=True)

# Example questions
ex_col1, ex_col2, ex_col3, ex_col4 = st.columns(4)
example_questions = [
    "What is AAPL price today?",
    "Compare Tesla and Microsoft in 2022",
    "Show me NVDA from 2020 to now",
    "Top 10 stocks by average volume",
    "Amazon price trend 2020–2023",
    "Biggest gainers in 2021",
    "What are today's top tech stocks?",
    "Banking stocks in 2019"
]

for i, (col, q) in enumerate(zip([ex_col1, ex_col2, ex_col3, ex_col4] * 2, example_questions)):
    with col:
        if st.button(q, key=f"ex_{i}"):
            st.session_state.selected_question = q

# Chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"], unsafe_allow_html=True)
        if message.get("sql"):
            with st.expander("🔍 Generated SQL"):
                st.code(message["sql"], language="sql")
        if message.get("data") is not None:
            df = message["data"]
            st.dataframe(df, use_container_width=True)
            if len(df) > 1:
                numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
                if numeric_cols:
                    if 'YEAR' in df.columns:
                        st.line_chart(df.set_index('YEAR')[numeric_cols[0]])
                    elif 'TICKER' in df.columns and len(df) <= 20:
                        st.bar_chart(df.set_index('TICKER')[numeric_cols[0]])
            csv = df.to_csv(index=False)
            st.download_button("📥 Download Results", csv, "query_results.csv", "text/csv",
                             key=f"dl_{message.get('id', id(message))}")

# Handle pre-selected question
if "selected_question" in st.session_state:
    prompt = st.session_state.selected_question
    del st.session_state.selected_question
else:
    prompt = st.chat_input("Ask about any stock, sector, trend, or market event...")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            try:
                # Enhancement 1: Intercept ROI / Investment queries
                if any(word in prompt.lower() for word in ['invested', 'invest', 'worth', 'roi']):
                    roi_params = extract_roi_params(prompt)
                    if "error" not in roi_params and all(k in roi_params for k in ['amount', 'ticker', 'start_date']):
                        roi_data, roi_err = handle_roi_query(roi_params)
                        if roi_err is None and roi_data is not None:
                            d = roi_data
                            sign = "+" if d['return_val'] >= 0 else ""
                            color = T['success'] if d['return_val'] >= 0 else T['danger']
                            
                            html_card = f"""
                            <div style="background:{T['card']}; border:1px solid {T['border']}; border-radius:8px; padding:15px; margin-bottom:15px;">
                                <h4 style="margin-top:0; color:{T['text_main']};">📈 Investment Calculator: {d['ticker']}</h4>
                                <table style="width:100%; text-align:left; color:{T['text_main']}; border-collapse: collapse;">
                                    <tr><td style="padding:4px 0; border-bottom:1px solid {T['border']};">Initial Investment ({d['start_date']})</td><td style="text-align:right; border-bottom:1px solid {T['border']};"><b>${d['amount']:,.2f}</b></td></tr>
                                    <tr><td style="padding:4px 0; border-bottom:1px solid {T['border']};">Start Price</td><td style="text-align:right; border-bottom:1px solid {T['border']};">${d['start_price']:,.2f}</td></tr>
                                    <tr><td style="padding:4px 0; border-bottom:1px solid {T['border']};">Shares Purchased</td><td style="text-align:right; border-bottom:1px solid {T['border']};">{d['shares']:,.2f}</td></tr>
                                    <tr><td style="padding:4px 0; border-bottom:1px solid {T['border']};">Current Price</td><td style="text-align:right; border-bottom:1px solid {T['border']};">${d['end_price']:,.2f}</td></tr>
                                    <tr><td style="padding:8px 0 0 0; font-size:1.1em;"><b>Current Value</b></td><td style="text-align:right; padding:8px 0 0 0; font-size:1.1em;"><b>${d['current_value']:,.2f}</b></td></tr>
                                    <tr><td style="padding:4px 0; color:{color};"><b>Total Return</b></td><td style="text-align:right; color:{color};"><b>{sign}${d['return_val']:,.2f} ({sign}{d['roi_pct']:.1f}%)</b></td></tr>
                                </table>
                            </div>
                            """
                            
                            insight_prompt = f"""You are a financial analyst. The user asked: {prompt}
Here is the math result:
Investment: ${d['amount']:,.2f} on {d['start_date']} at ${d['start_price']:,.2f} in {d['ticker']}.
Today's price: ${d['end_price']:,.2f}. Current value: ${d['current_value']:,.2f}.
Return: {sign}${d['return_val']:,.2f} ({sign}{d['roi_pct']:.1f}%).
Write a 3 sentence insightful analysis pointing out a key reason for the stock's performance since {d['start_date']} and briefly commenting on the return magnitude."""
                            insights, insight_model = call_llm(insight_prompt, "analysis")
                            ib = "⚡ Groq" if insight_model == "groq" else "🛡️ Cortex"
                            insights, insight_model = call_llm(insight_prompt, "analysis")
                            ib = "⚡ Groq" if insight_model == "groq" else "🛡️ Cortex"
                            analysis_html = f'<div class="insight-box"><div class="insight-label">⬡ AI Analysis — {ib} Intelligence</div>{insights}</div>'
                            
                            # Enhancement 4: Sentiment Badges
                            ins_lower = insights.lower()
                            bull_words = ["up", "gains", "positive", "outperforming", "bullish", "higher", "surge"]
                            bear_words = ["down", "decline", "negative", "underperforming", "bearish", "lower", "drop"]
                            bull_count = sum(1 for w in bull_words if w in ins_lower)
                            bear_count = sum(1 for w in bear_words if w in ins_lower)
                            
                            sentiment_html = ""
                            if bull_count > bear_count:
                                sentiment_html = '<span style="background-color:rgba(46,125,50,0.2);color:#81c784;border:1px solid #2e7d32;padding:2px 8px;border-radius:12px;font-size:0.8em;margin-left:5px;">🟢 BULLISH</span>'
                            elif bear_count > bull_count:
                                sentiment_html = '<span style="background-color:rgba(198,40,40,0.2);color:#e57373;border:1px solid #c62828;padding:2px 8px;border-radius:12px;font-size:0.8em;margin-left:5px;">🔴 BEARISH</span>'
                            else:
                                sentiment_html = '<span style="background-color:rgba(249,168,37,0.2);color:#fff59d;border:1px solid #f9a825;padding:2px 8px;border-radius:12px;font-size:0.8em;margin-left:5px;">🟡 NEUTRAL</span>'
                                
                            badges_html += sentiment_html
                            
                            final_output = f"{badges_html}<br>{html_card}\n{analysis_html}" if "html_card" in locals() else f"{badges_html}<br>{analysis_html}"
                            st.markdown(final_output, unsafe_allow_html=True)
                            st.session_state.messages.append({"role": "assistant", "content": final_output})
                            st.stop()
                            
                # Step 1: Extract tickers from the question
                tickers, extraction_model = extract_tickers_from_question(prompt)
                ai_badge_class = "badge-groq" if extraction_model == "groq" else "badge-mistral"
                ai_badge_text = "⚡ GROQ" if extraction_model == "groq" else "🛡️ MISTRAL"
                
                # Step 2: Rate limit check
                if len(tickers) > 10:
                    msg = "⚠️ Please limit comparisons to **10 stocks maximum** to optimize performance."
                    st.warning(msg)
                    st.session_state.messages.append({"role": "assistant", "content": msg})
                
                # Step 3: Smart data routing (if tickers found)
                elif len(tickers) > 0:
                    route_results = smart_data_router(tickers, fmp_api_key)
                    
                    if not snowflake_available:
                        st.warning("⚠️ Database temporarily unavailable (account security lockout). \n\nUsing live API data only. Historical data from 1962-2023 unavailable until Snowflake account is unlocked. Please wait 30-60 minutes or reset your password.")
                    
                    badges_html = ""
                    has_api_data = False
                    api_dfs = []
                    not_found_msgs = []
                    
                    for ticker, info in route_results.items():
                        if info["source"] == "not_found":
                            suggestions = info.get("suggestions", [])
                            sugg_text = ", ".join([f"**{s['ticker']}** ({s['name']})" for s in suggestions[:3]])
                            not_found_msgs.append(f"❌ Ticker `{ticker}` not found. Did you mean: {sugg_text}?")
                        elif info["source"] == "api":
                            badge_label = info.get("badge", "🔴 LIVE")
                            if "YAHOO" in badge_label:
                                badges_html += f'<span class="badge-yahoo">🟡 {ticker}: YAHOO</span> '
                            else:
                                badges_html += f'<span class="badge-live">🔴 {ticker}: LIVE</span> '
                            has_api_data = True
                            if info["api_data"] is not None:
                                api_dfs.append(info["api_data"])
                        elif info["source"] == "combined":
                            badges_html += f'<span class="badge-combined">🔄 {ticker}: COMBINED</span> '
                            has_api_data = True
                            if info["api_data"] is not None:
                                api_dfs.append(info["api_data"])
                        else:
                            badges_html += f'<span class="badge-historical">📊 {ticker}: HISTORICAL</span> '
                    
                    for msg in not_found_msgs:
                        st.markdown(msg)
                    
                    # Debug info
                    with st.expander("🔧 Debug: Data Routing Details"):
                        st.caption(f"Snowflake DB: {'🟢 Connected' if snowflake_available else '🔴 Offline (account locked?)'}")
                        st.caption(f"FMP API Key: {'🟢 Configured' if fmp_api_key else '🔴 Missing'}")
                        st.caption(f"Groq AI: {'🟢 Active' if groq_available else '🟡 Offline'}")
                        for t, info in route_results.items():
                            st.caption(f"{t}: source={info['source']}, error={info.get('error', 'None')}")
                    
                    valid_tickers = [t for t, i in route_results.items() if i["source"] != "not_found"]
                    
                    if valid_tickers:
                        timestamp = ""
                        if has_api_data:
                            timestamp = f' <span style="font-size:0.7rem;color:{T["text_muted"]};">Updated: just now</span>'
                        badges_html += f' <span class="{ai_badge_class}">{ai_badge_text}</span>{timestamp}'
                        st.markdown(badges_html, unsafe_allow_html=True)
                        
                        db_tickers = [t for t, i in route_results.items() if i["source"] in ("database", "combined")]
                        result_df = pd.DataFrame()
                        sql_query = ""
                        
                        if db_tickers and snowflake_available:
                            tickers_str_q = ", ".join([f"'{t}'" for t in db_tickers])
                            # Dynamic LIMIT based on ticker count for time-series
                            per_ticker_limit = 300
                            dynamic_limit = len(db_tickers) * per_ticker_limit
                            sql_prompt = f"""You are a SQL expert analyzing stock market data.
Database: FINANCE_AI_DB.STOCK_DATA.PRICES
Columns: date (DATE), ticker (VARCHAR), open (FLOAT), high (FLOAT), low (FLOAT), close (FLOAT), volume (FLOAT), adj_close (FLOAT)
Available tickers in DB: {', '.join(db_tickers)}
IMPORTANT: The database contains HISTORICAL data only (roughly 1962 to late 2023). For recent/current prices, the app uses a separate live API — do NOT try to query for dates after 2023.
User question: {prompt}
CRITICAL RULES:
1. Use ONLY Snowflake SQL syntax. Use CURRENT_DATE() not CURDATE(). Use DATE_FROM_PARTS() not MAKEDATE().
2. Generate EXACTLY ONE SQL SELECT statement. No semicolons. No UNION. No multiple queries.
3. Do NOT output any markdown, backticks, or conversational text (no "Note:", no explanations).
4. For 'today' or 'this year', get the MOST RECENT data available.
5. For multi-stock comparisons, use WHERE ticker IN ('TICK1','TICK2',...) — do NOT use separate queries.
6. For time-series queries (trends, performance, comparison over time): return raw daily rows with date, ticker, and price columns. Do NOT aggregate into a single row per ticker. Use ORDER BY date and LIMIT {dynamic_limit}.
7. For non-time-series queries, LIMIT results to 50 rows.
Generate the SQL now:"""
                            
                            sql_query, _ = call_llm(sql_prompt, "sql_generation")
                            sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
                            # Sanitize: ensure exactly 1 statement
                            if ';' in sql_query:
                                sql_query = sql_query.split(';')[0].strip()
                            # Strip any trailing conversational text (lines not starting with SQL keywords)
                            sql_lines = sql_query.split('\n')
                            sql_keywords = ('SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'ORDER', 'GROUP', 'HAVING', 'LIMIT', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'ON', 'AS', 'WITH', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', ')', '(', ',', 'UNION', 'BETWEEN', 'IN', 'NOT', 'LIKE', 'IS', 'NULL', 'ASC', 'DESC', 'DISTINCT', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'OVER', 'PARTITION', 'ROW_NUMBER', 'LAG', 'LEAD')
                            clean_lines = []
                            for line in sql_lines:
                                stripped = line.strip().upper()
                                if stripped and (stripped.startswith(sql_keywords) or stripped.startswith('--') or any(stripped.startswith(kw) for kw in sql_keywords)):
                                    clean_lines.append(line)
                                elif stripped and clean_lines:  # continuation lines (column names, etc.)
                                    clean_lines.append(line)
                            if clean_lines:
                                sql_query = '\n'.join(clean_lines)
                            with st.expander("🔍 Generated SQL"):
                                st.code(sql_query, language="sql")
                            try:
                                result_df = run_query(sql_query)
                            except Exception as sql_err:
                                pass  # Silent: API data fallback will handle this
                        
                        if api_dfs:
                            api_combined = pd.concat(api_dfs, ignore_index=True)
                            # Normalize DATE columns to pd.Timestamp to prevent type mismatch
                            if 'DATE' in api_combined.columns:
                                api_combined['DATE'] = pd.to_datetime(api_combined['DATE'], errors='coerce')
                            if not result_df.empty and 'DATE' in result_df.columns:
                                result_df['DATE'] = pd.to_datetime(result_df['DATE'], errors='coerce')
                            
                            if result_df.empty:
                                result_df = api_combined
                            else:
                                # ALWAYS merge API data with DB data for combined sources
                                # This ensures queries like "top 5 all time" include 2024-2025 data
                                common_cols = list(set(result_df.columns) & set(api_combined.columns))
                                if common_cols:
                                    result_df = pd.concat([result_df, api_combined[common_cols]], ignore_index=True)
                                    if 'DATE' in result_df.columns and 'TICKER' in result_df.columns:
                                        result_df = result_df.drop_duplicates(subset=['DATE', 'TICKER'], keep='last')
                                    result_df = result_df.sort_values('DATE', ascending=False).reset_index(drop=True) if 'DATE' in result_df.columns else result_df
                        
                        if len(result_df) > 0:
                            # Handle quarterly aggregation explicitly
                            if any(q_word in prompt.lower() for q_word in ["quarter", "quarterly", "q1", "q2", "q3", "q4"]):
                                try:
                                    if 'DATE' in result_df.columns:
                                        result_df['DATE'] = pd.to_datetime(result_df['DATE'])
                                        result_df['YEAR'] = result_df['DATE'].dt.year
                                        result_df['QUARTER'] = result_df['DATE'].dt.quarter
                                        
                                        q_df = result_df.groupby(['YEAR', 'QUARTER', 'TICKER']).agg(
                                            AVG_CLOSE=('CLOSE', 'mean'),
                                            QUARTER_HIGH=('HIGH', 'max'),
                                            QUARTER_LOW=('LOW', 'min'),
                                            TOTAL_VOLUME=('VOLUME', 'sum')
                                        ).reset_index()
                                        
                                        result_df = q_df.sort_values(['YEAR', 'QUARTER'])
                                except Exception as e:
                                    st.warning(f"Quarterly aggregation error: {e}")
                            
                            # Apply AI-driven filtering/sorting based on user prompt (e.g. "top 5 highest")
                            col_dtypes = {col: str(result_df[col].dtype) for col in result_df.columns}
                            filter_prompt = f"""You are a Python Pandas expert.
User asked: {prompt}
DataFrame 'df' has columns: {', '.join(result_df.columns)}
Column types: {col_dtypes}
Write ONLY a single line of Python code that modifies 'df' to answer the user's question (e.g. sorting, getting top N).
Return ONLY the code. No markdown, no explanations. 
IMPORTANT RULES:
1. Do NOT use .str accessors on non-string columns. DATE is datetime64, numeric columns are float64.
2. Do NOT invent new column names. You can ONLY use the columns listed above.
3. If the user asks for "share price" or "price", use the 'CLOSE' column.
If no filtering/sorting is needed, return EXACTLY: df
Example: df.nlargest(5, 'HIGH')
Example: df[df['DATE'] >= '2023-01-01']
Your code:"""
                            filter_code, _ = call_llm(filter_prompt, "sql_generation")
                            filter_code = filter_code.replace("```python", "").replace("```", "").strip()
                            
                            if filter_code and filter_code != "df":
                                try:
                                    # Safe evaluation context
                                    original_df = result_df.copy()
                                    local_env = {"df": result_df.copy(), "pd": pd}
                                    exec(f"filtered_df = {filter_code}", {}, local_env)
                                    import numpy as np
                                    result_df = local_env.get("filtered_df", result_df)
                                    if isinstance(result_df, pd.Series):
                                        result_df = result_df.to_frame()
                                    elif isinstance(result_df, (int, float, str, np.number)):
                                        result_df = pd.DataFrame({"Result": [result_df]})
                                        
                                    if result_df.empty and not original_df.empty:
                                        result_df = original_df # Fallback if AI filtering aggressively deleted everything
                                        
                                    with st.expander("🛠️ Data Transformation"):
                                        st.code(filter_code, language="python")
                                except Exception as e:
                                    pass  # Silent: unfiltered data still renders correctly

                            st.dataframe(result_df, use_container_width=True)
                            
                            show_chart = any(word in prompt.lower() for word in ['graph', 'chart', 'trend', 'plot', 'visual', 'compare', 'comparison', 'performance', 'show', 'over time', 'growth', 'return', 'annual'])
                            numeric_cols = result_df.select_dtypes(include=['float64', 'int64']).columns.tolist()
                            # Prefer CLOSE column for y-axis if available
                            chart_y = 'CLOSE' if 'CLOSE' in numeric_cols else (numeric_cols[0] if numeric_cols else None)
                            if show_chart and chart_y and not result_df.empty:
                                try:
                                    chart_x = 'DATE' if 'DATE' in result_df.columns else ('YEAR' if 'YEAR' in result_df.columns else result_df.index)
                                    color_col = 'TICKER' if 'TICKER' in result_df.columns and result_df['TICKER'].nunique() > 1 else None
                                    fig = px.line(result_df.sort_values('DATE') if 'DATE' in result_df.columns else result_df, x=chart_x, y=chart_y, color=color_col,
                                                  title=f"{'vs '.join(result_df['TICKER'].unique()) if color_col else ''} Price Trend")
                                    fig.update_layout(
                                        paper_bgcolor=T['plotly_paper'], plot_bgcolor=T['plotly_plot'],
                                        font=dict(color=T['plotly_text']),
                                        xaxis=dict(gridcolor=T['plotly_grid']),
                                        yaxis=dict(gridcolor=T['plotly_grid']),
                                        margin=dict(l=10, r=10, t=40, b=10), height=400)
                                    st.plotly_chart(fig, use_container_width=True)
                                except Exception as e:
                                    pass  # Silent chart error
                            
                            data_summary = result_df.head(10).to_string()
                            source_note = "Data includes live API data." if has_api_data else "Data from historical database."
                            insight_prompt = f"""You are a senior financial analyst at Goldman Sachs.
User asked: {prompt}
{source_note}
Data: {data_summary}
Write a 3-4 sentence professional analysis. Be specific with numbers."""
                            
                            insights, insight_model = call_llm(insight_prompt, "analysis")
                            ib = "⚡ Groq" if insight_model == "groq" else "🛡️ Cortex"
                            st.markdown(f"""
                            <div class="insight-box">
                                <div class="insight-label">⬡ AI Analysis — {ib} Intelligence</div>
                                {insights}
                            </div>
                            """, unsafe_allow_html=True)
                            
                            csv = result_df.to_csv(index=False)
                            st.download_button("📥 Download Results", csv, "results.csv", "text/csv",
                                             key=f"dl_new_{len(st.session_state.messages)}")
                            st.session_state.messages.append({
                                "role": "assistant", "content": f"{badges_html}<br>{insights}",
                                "sql": sql_query, "data": result_df,
                                "id": len(st.session_state.messages)
                            })
                        else:
                            st.warning("No data found for the requested tickers.")
                    elif not_found_msgs:
                        st.session_state.messages.append({"role": "assistant", "content": "\n".join(not_found_msgs)})
                
                # Enhancement 2: Stock Screener
                else:
                    if not snowflake_available:
                        st.warning("⚠️ Screener requires database access. Database is currently locked.")
                        st.stop()
                        
                    sql_prompt = f"""You are a SQL expert screening stock market data.
Database: FINANCE_AI_DB.STOCK_DATA.PRICES
Columns: date, ticker, open, high, low, close, volume
User question: {prompt}
Write a Snowflake SQL query to answer this screening request. 
Examples: "stocks that doubled in 2021", "top 5 volume".
Calculate percentage changes using MIN/MAX by date if needed.
Return ONLY raw SQL. Limit results to 20 rows."""
                    
                    sql_query, _ = call_llm(sql_prompt, "sql_generation")
                    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
                    with st.expander("🔍 Generated Screener SQL"):
                        st.code(sql_query, language="sql")
                        
                    try:
                        result_df = run_query(sql_query)
                        if result_df.empty:
                            st.info("No stocks matched your screening criteria.")
                            st.stop()
                            
                        st.dataframe(result_df, use_container_width=True)
                        
                        insight_prompt = f"""You are a financial analyst evaluating a stock screen.
User asked: {prompt}
Data returned (top 10 rows):
{result_df.head(10).to_string()}
Write a 3-sentence summary explaining why these types of stocks fit the criteria or market context for this period. Use emojis like 🚀 for big winners if applicable."""
                        
                        insights, insight_model = call_llm(insight_prompt, "analysis")
                        ib = "⚡ Groq" if insight_model == "groq" else "🛡️ Cortex"
                        
                        ins_lower = insights.lower()
                        bull_words = ["up", "gains", "positive", "outperforming", "bullish", "higher", "surge", "doubled"]
                        bear_words = ["down", "decline", "negative", "underperforming", "bearish", "lower", "drop"]
                        bull_count = sum(1 for w in bull_words if w in ins_lower)
                        bear_count = sum(1 for w in bear_words if w in ins_lower)
                        
                        sentiment_html = ""
                        if bull_count > bear_count:
                            sentiment_html = '<span style="background-color:rgba(46,125,50,0.2);color:#81c784;border:1px solid #2e7d32;padding:2px 8px;border-radius:12px;font-size:0.8em;">🟢 BULLISH SCREEN</span>'
                        elif bear_count > bull_count:
                            sentiment_html = '<span style="background-color:rgba(198,40,40,0.2);color:#e57373;border:1px solid #c62828;padding:2px 8px;border-radius:12px;font-size:0.8em;">🔴 BEARISH SCREEN</span>'
                        else:
                            sentiment_html = '<span style="background-color:rgba(249,168,37,0.2);color:#fff59d;border:1px solid #f9a825;padding:2px 8px;border-radius:12px;font-size:0.8em;">🟡 NEUTRAL SCREEN</span>'

                        st.markdown(f'{sentiment_html}<br><div class="insight-box"><div class="insight-label">⬡ AI Analysis — {ib} Intelligence</div>{insights}</div>', unsafe_allow_html=True)
                        
                        csv = result_df.to_csv(index=False)
                        st.download_button("📥 Download Results", csv, "screener_results.csv", "text/csv", key=f"dl_new_{len(st.session_state.messages)}")
                        
                        st.session_state.messages.append({
                            "role": "assistant", "content": f'{sentiment_html}<br>{insights}',
                            "sql": sql_query, "data": result_df,
                            "id": len(st.session_state.messages)
                        })
                    except Exception as sql_err:
                        st.warning(f"Screener query error: {sql_err}")
                        
            except Exception as e:
                st.error(f"Error processing context: {e}")

# Clear chat
if st.session_state.messages:
    if st.button("🗑️ Clear Conversation", key="clear"):
        st.session_state.messages = []
        st.rerun()

# ─── FOOTER ──────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="
    margin-top: 3rem;
    padding: 1.5rem 0 1rem;
    border-top: 1px solid {T['border']};
    display: flex;
    align-items: center;
    justify-content: space-between;
">
    <div style="font-family:'Playfair Display',serif; color:{T['gold']}; font-size:0.9rem; font-weight:600;">
        FinSight<span style="color:{T['text_muted']}">AI</span>
    </div>
    <div style="font-size:0.7rem; color:{T['text_muted']}; font-family:'JetBrains Mono',monospace; letter-spacing:0.06em;">
        SNOWFLAKE CORTEX AI &nbsp;·&nbsp; GROQ AI &nbsp;·&nbsp; FMP LIVE DATA &nbsp;·&nbsp; 29.7M RECORDS
    </div>
    <div style="font-size:0.7rem; color:{T['text_muted']};">
        Not financial advice. For research purposes only.
    </div>
</div>
""", unsafe_allow_html=True)