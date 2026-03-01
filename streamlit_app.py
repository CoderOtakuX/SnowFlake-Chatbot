import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import snowflake.connector
from datetime import date, timedelta
import os

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
    creds = get_credentials()
    return snowflake.connector.connect(
        user=creds["user"],
        password=creds["password"],
        account=creds["account"],
        warehouse=creds["warehouse"],
        database="FINANCE_AI_DB",
        schema="STOCK_DATA"
    )

def run_query(query):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

def cortex_complete(prompt):
    safe = prompt.replace("'", "\\'").replace("$$", "")
    result = run_query(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', '{safe}') as response
    """)
    return result.iloc[0]['RESPONSE']

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
    nav_options = ["📊 Analysis", " Research"]
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

# ─── PAGE ROUTING ─────────────────────────────────────────────────────────────
current_page = st.session_state.get("nav_page", "📊 Analysis")

if current_page == "🔬 Research":
    st.markdown("### 🔬 AI Research Terminal")
    st.info("Deep-dive analysis powered by Cortex AI")
    research_topic = st.text_input("Enter a stock or sector to research deeply", "AAPL")
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
            st.error(f"Error loading price data: {e}")

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
            st.error(f"Error loading candlestick data: {e}")

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
            st.error(f"Error loading volume data: {e}")

# ─── AI CHAT SECTION ─────────────────────────────────────────────────────────
st.markdown(f"""
<div class="section-header">
    <span class="section-title">AI Research Assistant</span>
    <div class="section-line"></div>
    <span class="gold-tag">Cortex AI</span>
</div>
""", unsafe_allow_html=True)

# Example questions
ex_col1, ex_col2, ex_col3, ex_col4 = st.columns(4)
example_questions = [
    "What was Apple's highest price in 2023?",
    "Compare Tesla and Microsoft in 2022",
    "Top 10 stocks by average volume",
    "Amazon price trend 2020–2023",
    "Biggest gainers in 2021",
    "Google volatility over the years",
    "IBM performance during COVID",
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
        st.markdown(message["content"])
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
                sql_prompt = f"""You are a SQL expert analyzing stock market data.

Database: FINANCE_AI_DB.STOCK_DATA.PRICES
Columns: date (DATE), ticker (VARCHAR), open (FLOAT), high (FLOAT), low (FLOAT), close (FLOAT), volume (FLOAT), adj_close (FLOAT)

User question: {prompt}

Generate ONLY a valid SQL SELECT query. No explanation, no markdown, just raw SQL.
Limit results to 20 rows. Use proper aggregations and ORDER BY."""

                sql_query = cortex_complete(sql_prompt)
                sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

                with st.expander("🔍 Generated SQL"):
                    st.code(sql_query, language="sql")

                try:
                    result_df = run_query(sql_query)

                    if len(result_df) > 0:
                        st.dataframe(result_df, use_container_width=True)

                        # Auto chart
                        numeric_cols = result_df.select_dtypes(include=['float64', 'int64']).columns.tolist()
                        if numeric_cols and len(result_df) > 1:
                            if 'YEAR' in result_df.columns:
                                fig = px.line(result_df, x='YEAR', y=numeric_cols[0])
                                fig.update_layout(
                                    paper_bgcolor=T['plotly_paper'], plot_bgcolor=T['plotly_plot'],
                                    font=dict(color=T['plotly_text']),
                                    xaxis=dict(gridcolor=T['plotly_grid']),
                                    yaxis=dict(gridcolor=T['plotly_grid']),
                                    margin=dict(l=10, r=10, t=20, b=10), height=300
                                )
                                fig.update_traces(line_color=T['gold'])
                                st.plotly_chart(fig, use_container_width=True)
                            elif 'TICKER' in result_df.columns and len(result_df) <= 20:
                                fig = px.bar(result_df, x='TICKER', y=numeric_cols[0],
                                           color_discrete_sequence=[T['gold']])
                                fig.update_layout(
                                    paper_bgcolor=T['plotly_paper'], plot_bgcolor=T['plotly_plot'],
                                    font=dict(color=T['plotly_text']),
                                    xaxis=dict(gridcolor=T['plotly_grid']),
                                    yaxis=dict(gridcolor=T['plotly_grid']),
                                    margin=dict(l=10, r=10, t=20, b=10), height=300
                                )
                                st.plotly_chart(fig, use_container_width=True)

                        # AI insight
                        insight_prompt = f"""You are a senior financial analyst at Goldman Sachs.
User asked: {prompt}
Data: {result_df.head(10).to_string()}

Write a 3-4 sentence professional analysis. Be specific with numbers. Cover: what the data shows, key pattern, investment implication."""

                        insights = cortex_complete(insight_prompt)

                        st.markdown(f"""
                        <div class="insight-box">
                            <div class="insight-label">⬡ AI Analysis — Cortex Intelligence</div>
                            {insights}
                        </div>
                        """, unsafe_allow_html=True)

                        csv = result_df.to_csv(index=False)
                        st.download_button("📥 Download Results", csv, "results.csv", "text/csv",
                                         key=f"dl_new_{len(st.session_state.messages)}")

                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": insights,
                            "sql": sql_query,
                            "data": result_df,
                            "id": len(st.session_state.messages)
                        })
                    else:
                        msg = "No data found. Try a specific ticker (AAPL, TSLA) or different date range."
                        st.warning(msg)
                        st.session_state.messages.append({"role": "assistant", "content": msg, "sql": sql_query})

                except Exception as sql_err:
                    st.error(f"Query error: {sql_err}")
                    fallback = "Query failed. Try rephrasing with specific tickers (e.g. AAPL, MSFT) or time periods (e.g. 2022, Q1 2023)."
                    st.info(fallback)
                    st.session_state.messages.append({"role": "assistant", "content": fallback, "sql": sql_query})

            except Exception as e:
                st.error(f"Error: {e}")

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
        SNOWFLAKE CORTEX AI &nbsp;·&nbsp; 29.7M RECORDS &nbsp;·&nbsp; 1962–2023
    </div>
    <div style="font-size:0.7rem; color:{T['text_muted']};">
        Not financial advice. For research purposes only.
    </div>
</div>
""", unsafe_allow_html=True)