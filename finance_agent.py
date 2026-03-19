# ═══════════════════════════════════════════════════════════════
# FinSight AI — Clean LLM + Yahoo Finance Agent
# ═══════════════════════════════════════════════════════════════

import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os
import concurrent.futures

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 0. GROQ CLIENT SETUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_groq_client():
    """Get Groq client using available secrets."""
    from groq import Groq

    # Try Streamlit secrets first, then env var, then dot.env file
    api_key = None

    try:
        api_key = st.secrets["GROQ_API_KEY"]
    except Exception:
        pass

    if not api_key:
        api_key = os.environ.get("GROQ_API_KEY")

    if not api_key:
        # Try loading from dot.env manually
        dotenv_path = os.path.join(os.path.dirname(__file__), "dot.env")
        if os.path.exists(dotenv_path):
            with open(dotenv_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("GROQ_API_KEY"):
                        api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break

    if not api_key:
        st.error("⚠️ Groq API key not found. Add GROQ_API_KEY to `.streamlit/secrets.toml` or `dot.env`.")
        return None

    return Groq(api_key=api_key)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. STOCK UNIVERSES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FULL_COVERAGE = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B',
    'LLY', 'V', 'UNH', 'XOM', 'JPM', 'JNJ', 'WMT', 'MA', 'PG', 'AVGO',
    'HD', 'CVX', 'MRK', 'ABBV', 'KO', 'COST', 'PEP', 'ADBE', 'CRM',
    'NFLX', 'MCD', 'ACN', 'CSCO', 'TMO', 'ABT', 'AMD', 'ORCL', 'INTC',
    'NKE', 'CMCSA', 'DIS', 'PFE', 'VZ', 'TXN', 'PM', 'QCOM', 'COP',
    'UNP', 'HON', 'INTU', 'LOW', 'UPS', 'RTX', 'NEE', 'AMGN', 'SPGI',
    'GS', 'BA', 'MS', 'SBUX', 'ELV', 'CAT', 'BKNG', 'T', 'BLK', 'MDT',
    'AXP', 'DE', 'SYK', 'GILD', 'VRTX', 'MDLZ', 'ADP', 'TJX', 'LRCX',
    'CI', 'REGN', 'ADI', 'MMC', 'SCHW', 'ISRG', 'AMT', 'CVS', 'NOW',
    'ZTS', 'PANW', 'CB', 'MO', 'PLD', 'SO', 'DUK', 'BDX', 'ITW', 'BSX',
    'CME', 'APH', 'WM', 'HCA', 'TT', 'USB',
]

INDIAN_LARGE_CAP = [
    'TCS.NS', 'INFY.NS', 'WIPRO.NS', 'HCLTECH.NS', 'TECHM.NS',
    'HDFCBANK.NS', 'ICICIBANK.NS', 'SBIN.NS', 'AXISBANK.NS', 'KOTAKBANK.NS',
    'RELIANCE.NS', 'ONGC.NS', 'NTPC.NS', 'POWERGRID.NS', 'ADANIGREEN.NS',
    'HINDUNILVR.NS', 'ITC.NS', 'BAJAJFINSV.NS', 'TITAN.NS', 'NESTLEIND.NS',
    'TATAMOTORS.NS', 'MARUTI.NS', 'M&M.NS', 'BAJAJ-AUTO.NS', 'HEROMOTOCO.NS',
    'SUNPHARMA.NS', 'DRREDDY.NS', 'CIPLA.NS', 'DIVISLAB.NS', 'BIOCON.NS',
    'LT.NS', 'ULTRACEMCO.NS', 'ADANIPORTS.NS', 'BHARTIARTL.NS',
    'TATASTEEL.NS', 'HINDALCO.NS', 'VEDL.NS', 'JSWSTEEL.NS',
]

US_MID_CAP = [
    'PLTR', 'SNOW', 'DDOG', 'CRWD', 'NET', 'ZS', 'OKTA', 'DKNG',
    'RBLX', 'U', 'COIN', 'SQ', 'HOOD', 'AFRM', 'UPST',
    'MRNA', 'BIIB', 'ILMN',
    'TFC', 'PNC', 'COF', 'MTB', 'FITB',
    'TGT', 'DG', 'DLTR', 'ULTA', 'ROST',
    'GE', 'EMR', 'ETN', 'PH',
]

INDIAN_MID_CAP = [
    'PERSISTENT.NS', 'COFORGE.NS', 'MPHASIS.NS',
    'BAJFINANCE.NS', 'CHOLAFIN.NS', 'PFC.NS', 'RECLTD.NS',
    'GODREJCP.NS', 'DABUR.NS', 'MARICO.NS', 'TATACONSUM.NS',
    'ADANIENT.NS', 'DLF.NS', 'OBEROIRLTY.NS', 'GODREJPROP.NS',
]


def _select_tickers(market="us", include_midcap=False):
    """Return ticker list based on market and mid-cap flag."""
    if market == "india":
        return INDIAN_LARGE_CAP + (INDIAN_MID_CAP if include_midcap else [])
    return FULL_COVERAGE + (US_MID_CAP if include_midcap else [])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. YAHOO FINANCE DATA FUNCTIONS  (concurrent for speed)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _fetch_intraday_single(ticker):
    """Fetch 2-day history for one ticker (used in thread pool)."""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="2d")
        if len(hist) >= 2:
            yc = hist['Close'].iloc[-2]
            tc = hist['Close'].iloc[-1]
            pct = ((tc - yc) / yc) * 100
            return {
                "ticker": ticker,
                "change_percent": round(pct, 2),
                "yesterday_close": round(yc, 2),
                "current_price": round(tc, 2),
                "volume": int(hist['Volume'].iloc[-1]),
            }
    except Exception:
        pass
    return None


def _fetch_yearly_single(ticker, year):
    """Fetch yearly history for one ticker (used in thread pool)."""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(start=f"{year}-01-01", end=f"{year}-12-31")
        if len(hist) >= 50:
            sp = hist['Close'].iloc[0]
            ep = hist['Close'].iloc[-1]
            ret = ((ep - sp) / sp) * 100
            if -95 <= ret <= 500:
                return {
                    "ticker": ticker,
                    "return_percent": round(ret, 2),
                    "start_price": round(sp, 2),
                    "end_price": round(ep, 2),
                    "avg_volume": int(hist['Volume'].mean()),
                    "trading_days": len(hist),
                }
    except Exception:
        pass
    return None


def _concurrent_fetch(tickers, worker_fn, label="stocks"):
    """Run worker_fn across tickers with a progress bar and thread pool."""
    results = []
    total = len(tickers)
    progress_bar = st.progress(0)
    status_text = st.empty()

    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(worker_fn, t): t for t in tickers}
        for future in concurrent.futures.as_completed(futures):
            completed += 1
            progress_bar.progress(completed / total)
            status_text.text(f"📊 Scanning {label}… ({completed}/{total})")
            res = future.result()
            if res is not None:
                results.append(res)

    progress_bar.empty()
    status_text.empty()
    return results


# ── public tool functions ──────────────────────────────────────────

def get_top_gainers_today(limit=10, market="us", include_midcap=False):
    """Get today's top gaining stocks."""
    tickers = _select_tickers(market, include_midcap)
    results = _concurrent_fetch(tickers, _fetch_intraday_single, "today's movers")
    results.sort(key=lambda x: x['change_percent'], reverse=True)
    return results[:limit]


def get_top_losers_today(limit=10, market="us", include_midcap=False):
    """Get today's top losing stocks."""
    tickers = _select_tickers(market, include_midcap)
    results = _concurrent_fetch(tickers, _fetch_intraday_single, "today's movers")
    results.sort(key=lambda x: x['change_percent'])          # ascending
    return results[:limit]


def get_best_stocks_year(year, limit=10, market="us", include_midcap=False):
    """Get best performing stocks for a specific year."""
    tickers = _select_tickers(market, include_midcap)
    worker = lambda t: _fetch_yearly_single(t, year)
    results = _concurrent_fetch(tickers, worker, f"{year} performance")
    results.sort(key=lambda x: x['return_percent'], reverse=True)
    return results[:limit]


def get_worst_stocks_year(year, limit=10, market="us", include_midcap=False):
    """Get worst performing stocks for a specific year."""
    tickers = _select_tickers(market, include_midcap)
    worker = lambda t: _fetch_yearly_single(t, year)
    results = _concurrent_fetch(tickers, worker, f"{year} performance")
    results.sort(key=lambda x: x['return_percent'])           # ascending
    return results[:limit]


def compare_stocks(tickers, year):
    """Compare performance of multiple stocks for a specific year."""
    results = []
    for ticker in tickers:
        row = _fetch_yearly_single(ticker, year)
        if row:
            results.append(row)
        else:
            results.append({"ticker": ticker, "error": f"Insufficient data for {year}"})
    return results


def get_stock_performance(ticker, start_year, end_year=None):
    """Get detailed performance for a single stock."""
    if end_year is None:
        end_year = start_year
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(start=f"{start_year}-01-01", end=f"{end_year}-12-31")
        if len(hist) < 50:
            return {"error": f"Insufficient data for {ticker} in {start_year}–{end_year}"}
        sp = hist['Close'].iloc[0]
        ep = hist['Close'].iloc[-1]
        return {
            "ticker": ticker,
            "start_price": round(sp, 2),
            "end_price": round(ep, 2),
            "return_percent": round(((ep - sp) / sp) * 100, 2),
            "trading_days": len(hist),
            "start_date": str(hist.index[0].date()),
            "end_date": str(hist.index[-1].date()),
            "high": round(hist['High'].max(), 2),
            "low": round(hist['Low'].min(), 2),
            "avg_volume": int(hist['Volume'].mean()),
        }
    except Exception as e:
        return {"error": str(e)}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. TOOL SCHEMAS FOR GROQ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

YAHOO_FINANCE_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_top_gainers_today",
            "description": "Get today's top gaining stocks with real-time data. Use for: 'top stocks today', 'best gainers', 'top movers', 'biggest gains'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit":          {"type": "integer", "description": "Number of results (default 10, max 20)", "default": 10},
                    "market":         {"type": "string",  "enum": ["us", "india"], "description": "'us' or 'india'", "default": "us"},
                    "include_midcap": {"type": "boolean", "description": "Include mid-cap stocks (slower)", "default": False},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_top_losers_today",
            "description": "Get today's top losing stocks. Use for: 'worst stocks today', 'top losers', 'biggest declines'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit":          {"type": "integer", "description": "Number of results (default 10)", "default": 10},
                    "market":         {"type": "string",  "enum": ["us", "india"], "default": "us"},
                    "include_midcap": {"type": "boolean", "default": False},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_best_stocks_year",
            "description": "Best performing stocks for a specific year. Use for: 'best stocks 2022', 'top performers 2021'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "year":           {"type": "integer", "description": "Year to analyse"},
                    "limit":          {"type": "integer", "default": 10},
                    "market":         {"type": "string",  "enum": ["us", "india"], "default": "us"},
                    "include_midcap": {"type": "boolean", "default": False},
                },
                "required": ["year"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_worst_stocks_year",
            "description": "Worst performing stocks for a specific year. Use for: 'worst stocks 2022', 'biggest losers 2021'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "year":           {"type": "integer", "description": "Year to analyse"},
                    "limit":          {"type": "integer", "default": 10},
                    "market":         {"type": "string",  "enum": ["us", "india"], "default": "us"},
                    "include_midcap": {"type": "boolean", "default": False},
                },
                "required": ["year"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_stocks",
            "description": "Compare multiple stocks for a year. Use for: 'compare AAPL vs MSFT 2022', 'Tesla vs Apple 2021'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "tickers": {"type": "array", "items": {"type": "string"}, "description": "Ticker list e.g. ['AAPL','MSFT']"},
                    "year":    {"type": "integer"},
                },
                "required": ["tickers", "year"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_stock_performance",
            "description": "Detailed performance for one stock. Use for: 'AAPL performance 2022', 'how did TSLA do in 2021'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker":     {"type": "string",  "description": "Ticker symbol"},
                    "start_year": {"type": "integer", "description": "Start year"},
                    "end_year":   {"type": "integer", "description": "End year (optional)"},
                },
                "required": ["ticker", "start_year"],
            },
        },
    },
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. LLM AGENT — FUNCTION-CALLING LOOP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TOOL_DISPATCH = {
    "get_top_gainers_today":  get_top_gainers_today,
    "get_top_losers_today":   get_top_losers_today,
    "get_best_stocks_year":   get_best_stocks_year,
    "get_worst_stocks_year":  get_worst_stocks_year,
    "compare_stocks":         compare_stocks,
    "get_stock_performance":  get_stock_performance,
}

SYSTEM_PROMPT = """You are a financial data assistant with access to real-time Yahoo Finance data.

Current date: {date}

Available functions:
- get_top_gainers_today: "top stocks today", "best gainers today"
- get_top_losers_today:  "worst stocks today", "top losers today", "biggest declines"
- get_best_stocks_year:  "best stocks 2022", "top performers 2021" (historical)
- get_worst_stocks_year: "worst stocks 2022", "biggest losers 2021" (historical)
- compare_stocks:        "compare AAPL vs MSFT 2022"
- get_stock_performance: "AAPL performance 2022"

Rules:
- For "today" queries → gainers or losers function.
- For past-year queries (2020-2025) → yearly functions.
- If "india"/"nse" → market="india".
- If "mid cap"/"midcap" → include_midcap=true.
- Always call a function. Do NOT guess data.
"""


def process_query_with_agent(user_query: str):
    """LLM picks a tool → execute → LLM analyses results."""
    client = get_groq_client()
    if client is None:
        return None

    # ── Step 1: LLM picks the function ──
    with st.spinner("🤖 AI analysing your query…"):
        try:
            resp = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT.format(date=datetime.now().strftime("%Y-%m-%d"))},
                    {"role": "user",   "content": user_query},
                ],
                tools=YAHOO_FINANCE_TOOLS,
                tool_choice="auto",
                temperature=0.1,
            )
        except Exception as e:
            st.error(f"❌ LLM error: {e}")
            return None

    tool_calls = resp.choices[0].message.tool_calls

    if not tool_calls:
        st.warning("🤔 Couldn't determine the right function. Try rephrasing.")
        if resp.choices[0].message.content:
            st.write(resp.choices[0].message.content)
        return None

    # ── Step 2: Execute every tool call ──
    function_results = []
    for tc in tool_calls:
        fn_name = tc.function.name
        fn_args = json.loads(tc.function.arguments)

        st.info(f"🔄 **Calling:** `{fn_name}`")

        # Friendly arg display
        parts = []
        if "year" in fn_args:
            parts.append(f"Year: {fn_args['year']}")
        if "limit" in fn_args:
            parts.append(f"Limit: {fn_args['limit']}")
        if fn_args.get("market"):
            parts.append(f"Market: {fn_args['market'].upper()}")
        if fn_args.get("include_midcap"):
            parts.append("⚠️ Mid-cap included (may take 45-90 s)")
        if fn_args.get("tickers"):
            parts.append(f"Tickers: {', '.join(fn_args['tickers'])}")
        if fn_args.get("ticker"):
            parts.append(f"Ticker: {fn_args['ticker']}")
        if parts:
            st.caption(" | ".join(parts))

        with st.spinner("⏳ Fetching data from Yahoo Finance…"):
            func = TOOL_DISPATCH.get(fn_name)
            if func:
                try:
                    result = func(**fn_args)
                except Exception as e:
                    result = {"error": str(e)}
            else:
                result = {"error": f"Unknown function: {fn_name}"}

        function_results.append({"function": fn_name, "args": fn_args, "result": result})

    # ── Step 3: LLM writes the analysis ──
    first = function_results[0]
    data_for_llm = first["result"]
    if isinstance(data_for_llm, list):
        data_for_llm = data_for_llm[:10]       # cap so token stays small

    with st.spinner("💡 AI generating insights…"):
        try:
            analysis_resp = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            f"You are a financial analyst. Current date: {datetime.now().strftime('%Y-%m-%d')}. "
                            "Provide: 1) 2-3 sentence summary, 2) key insights, 3) notable observations. "
                            "Be concise and professional."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f'User asked: "{user_query}"\n\n'
                            f"Function: {first['function']}\n"
                            f"Args: {json.dumps(first['args'])}\n\n"
                            f"Results:\n{json.dumps(data_for_llm, indent=2)}"
                        ),
                    },
                ],
                temperature=0.3,
                max_tokens=500,
            )
            analysis = analysis_resp.choices[0].message.content
        except Exception as e:
            analysis = f"_Analysis unavailable: {e}_"

    return {"function_results": function_results, "analysis": analysis}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. VISUALISATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COLUMN_MAP = {
    "ticker": "Ticker",
    "change_percent": "Change (%)",
    "return_percent": "Return (%)",
    "current_price": "Price",
    "yesterday_close": "Prev Close",
    "start_price": "Start Price",
    "end_price": "End Price",
    "volume": "Volume",
    "avg_volume": "Avg Volume",
    "trading_days": "Days",
}


def display_results(function_results, analysis):
    """Render results: table → chart → download → AI analysis."""

    for fr in function_results:
        fn_name = fr["function"]
        data = fr["result"]

        if isinstance(data, dict) and "error" in data:
            st.error(f"❌ {data['error']}")
            continue

        # ── list of dicts → DataFrame ──
        if isinstance(data, list) and data:
            df = pd.DataFrame(data).rename(columns=COLUMN_MAP)

            st.success(f"✅ **{len(df)} stocks found** | Updated {datetime.now().strftime('%I:%M %p')}")
            st.dataframe(df, use_container_width=True, hide_index=True)

            # bar chart
            y_col = "Change (%)" if "Change (%)" in df.columns else "Return (%)" if "Return (%)" in df.columns else None
            if y_col:
                title = "Today's Performance" if y_col == "Change (%)" else f"Performance ({fr['args'].get('year', '')})"
                fig = px.bar(
                    df, x="Ticker", y=y_col, title=title,
                    color=y_col, color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
                )
                fig.update_layout(height=420, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

            csv = df.to_csv(index=False)
            st.download_button(
                "📥 Download CSV", csv,
                file_name=f"finsight_{fn_name}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )

        # ── single-stock dict ──
        elif isinstance(data, dict) and "error" not in data:
            st.success("✅ **Stock Performance**")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Ticker", data.get("ticker", "N/A"))
            c2.metric("Return", f"{data.get('return_percent', 0)}%")
            c3.metric("Start", f"${data.get('start_price', 0)}")
            c4.metric("End", f"${data.get('end_price', 0)}")

    st.markdown("---")
    st.markdown("### 💡 AI Analysis")
    st.markdown(analysis)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. STREAMLIT UI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    st.set_page_config(
        page_title="FinSight AI — Financial Intelligence",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    # ──── CSS ────
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    .main { padding: 1.5rem 2rem; }

    /* gradient header bar */
    .header-bar {
        background: linear-gradient(135deg, #0f2027, #203a43, #2c5364);
        padding: 2rem 2.5rem;
        border-radius: 16px;
        margin-bottom: 1.5rem;
        color: white;
    }
    .header-bar h1 { margin: 0; font-size: 2.2rem; }
    .header-bar p  { margin: 0.3rem 0 0; opacity: 0.85; font-size: 1rem; }

    /* query box */
    .stTextInput > div > div > input {
        font-size: 1.05rem;
        border-radius: 12px;
        padding: 0.75rem 1rem;
        border: 2px solid #2c5364;
    }
    .stTextInput > div > div > input:focus {
        border-color: #FF6B35;
        box-shadow: 0 0 0 3px rgba(255,107,53,0.15);
    }

    /* buttons */
    .stButton>button {
        background: linear-gradient(135deg, #FF6B35, #F7C948);
        color: #1a1a2e; font-weight: 600;
        border: none; border-radius: 10px;
        padding: 0.55rem 1.4rem;
        transition: transform 0.15s, box-shadow 0.15s;
    }
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(255,107,53,0.35);
    }

    /* metrics */
    [data-testid="stMetricValue"] { font-size: 1.3rem; font-weight: 700; }

    /* download btn */
    .stDownloadButton>button {
        background: #2c5364; color: white;
        border-radius: 8px; font-weight: 500;
    }

    /* expander */
    .streamlit-expanderHeader { font-weight: 600; }
    </style>
    """, unsafe_allow_html=True)

    # ──── Header ────
    st.markdown("""
    <div class="header-bar">
        <h1>📊 FinSight AI</h1>
        <p>Enterprise Financial Intelligence Platform &nbsp;·&nbsp; Powered by AI + Yahoo Finance</p>
    </div>
    """, unsafe_allow_html=True)

    # ──── Example queries ────
    with st.expander("💡 Example Queries", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("""
            **Today's Market**
            - "top stocks today"
            - "worst losers today"
            - "top indian stocks today"
            """)
        with col2:
            st.markdown("""
            **Historical**
            - "best stocks 2022"
            - "worst stocks 2021"
            - "best indian stocks 2020"
            """)
        with col3:
            st.markdown("""
            **Compare / Single**
            - "compare AAPL vs MSFT 2022"
            - "AAPL performance 2022"
            - "best mid cap stocks today"
            """)

    st.markdown("---")

    # ──── Query input ────
    user_query = st.text_input(
        "💬 Ask about stocks, sectors, trends, or market events…",
        placeholder="Example: top stocks today",
        key="query_input",
    )

    if user_query:
        st.markdown(f"### 🔍 Query: *{user_query}*")
        result = process_query_with_agent(user_query)
        if result:
            display_results(result["function_results"], result["analysis"])

    # ──── Footer ────
    st.markdown("---")
    st.caption(
        "**FinSight AI** · Data: Yahoo Finance · Analysis: Groq AI · "
        "Not financial advice. For research purposes only."
    )


if __name__ == "__main__":
    main()
