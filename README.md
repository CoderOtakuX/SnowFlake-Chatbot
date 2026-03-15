# 💰 Financial AI Analyst

A powerful, AI-driven stock market analysis tool built with Streamlit and Snowflake Cortex AI. This platform allows users to query over 50 years of market data (1973-Present) across 29.7+ million records using natural language.

---

## 🚀 Features

- **Natural Language to SQL**: Ask complex financial questions in plain English, and the integrated AI translates them into valid Snowflake SQL queries dynamically.
- **Auto-generated Visualizations**: Automatically generates interactive Plotly line charts (for time-series data) and comparative overlays based on query results spanning multiple companies.
- **AI Insights**: Provides 3-4 sentence professional analyses of the returned data using advanced LLMs (Groq & Mistral), highlighting key patterns and investment implications.
- **Massive Dataset**: Connected to a Snowflake database containing historical daily price data (Open, High, Low, Close, Volume) for thousands of tickers, automatically falling back to FMP and Yahoo Finance APIs for current real-time metrics.
- **Data Exporting**: Built-in CSV support to seamlessly download the actual market data returned from your specific conversational queries.
- **Interactive UI**: Clean, modern interface with a responsive chat layout, data coverage matrices, timeline selectors (including custom date picker ranges), and helpful sidebar metrics.

---

## 🛠️ Tech Stack

- **Frontend**: [Streamlit](https://streamlit.io/) (Python web framework)
- **Data & AI Engine**: [Snowflake](https://www.snowflake.com/) & Snowflake Cortex AI
- **Fast Reasoning LLM**: [Groq](https://groq.com)
- **Data Manipulation**: [Pandas](https://pandas.pydata.org/)
- **Visualizations**: [Plotly](https://plotly.com/python/) Express

---

## 📋 Prerequisites

To run this application, you need:

1. **Python 3.8+**
2. A **Snowflake Account** with:
   - Access to Cortex AI (`SNOWFLAKE.CORTEX.COMPLETE` function)
   - A database named `FINANCE_AI_DB`
   - A schema named `STOCK_DATA`
   - A table named `PRICES` with columns: `date` (DATE), `ticker` (VARCHAR), `open` (FLOAT), `high` (FLOAT), `low` (FLOAT), `close` (FLOAT), `volume` (FLOAT)
3. API Keys for **Groq** and **Financial Modeling Prep** optionally injected into a `.env` file for redundancy logic.

---

## ⚙️ Local Setup

1. **Clone the repository:**
   ```bash
   git clone <your-github-repo-url>
   cd <repository-folder>
   ```

2. **Install dependencies:**
   It is recommended to use a virtual environment.
   ```bash
   pip install streamlit pandas snowflake-connector-python plotly groq yfinance requests python-dotenv
   ```

3. **Configure API Secrets:**
   Ensure you create a `.env` file at the root of the project with your `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE`, `FMP_API_KEY`, and `GROQ_API_KEY`.

4. **Run the application:**
   ```bash
   streamlit run streamlit_app.py
   ```

---

## 🧠 How it Works

1. **User Input:** The user types a question like *"Compare performance trends for NVDA and AMD"*
2. **SQL Generation:** The AI logic takes the database schema and the user's question, and generates a raw Snowflake SQL query using `WHERE ticker IN ('NVDA', 'AMD')`.
3. **Execution:** The generated query is executed against the historical Snowflake database.
4. **Visualization:** If the execution is successful, the data is returned as a Pandas DataFrame, rendered natively, and graphed as a multi-line Plotly chart.
5. **Insights:** The LLM parses the top rows of the resulting DataFrame to generate a contextual human-readable summary, attaching visual `BULLISH` or `BEARISH` sentiment tags.

---

## 🤝 Contributing

Contributions, issues, and feature requests are welcome!

## 📄 License

This project is open-source and available under the [MIT License](LICENSE).

---
*Disclaimer: This application is for educational and research purposes only. It does not constitute financial advice.*
