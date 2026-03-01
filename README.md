# 💰 Financial AI Analyst

A powerful, AI-driven stock market analysis tool built with Streamlit and Snowflake Cortex AI. This platform allows users to query over 62 years of market data (1962-2023) across 29.7+ million records using natural language.

---

## 🚀 Features

- **Natural Language to SQL**: Ask complex financial questions in plain English, and Snowflake Cortex AI translates them into valid SQL queries.
- **Auto-generated Visualizations**: Automatically generates line charts (for time-series data) and bar charts (for comparisons) based on query results.
- **AI Insights**: Provides 3-4 sentence professional analyses of the returned data, highlighting key patterns and investment implications.
- **Massive Dataset**: Connected to a Snowflake database containing historical daily price data (Open, High, Low, Close, Volume) for thousands of tickers.
- **Interactive UI**: Clean, modern interface with a responsive chat layout and helpful sidebar metrics.

---

## 🛠️ Tech Stack

- **Frontend**: [Streamlit](https://streamlit.io/) (Python web framework)
- **Data & AI Engine**: [Snowflake](https://www.snowflake.com/) & Snowflake Cortex AI
- **Data Manipulation**: [Pandas](https://pandas.pydata.org/)
- **Visualizations**: Streamlit Native Charts

---

## 📋 Prerequisites

To run this application, you need:

1. **Python 3.8+**
2. A **Snowflake Account** with:
   - Access to Cortex AI (`SNOWFLAKE.CORTEX.COMPLETE` function)
   - A database named `FINANCE_AI_DB`
   - A schema named `STOCK_DATA`
   - A table named `PRICES` with columns: `date` (DATE), `ticker` (VARCHAR), `open` (FLOAT), `high` (FLOAT), `low` (FLOAT), `close` (FLOAT), `volume` (FLOAT)

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
   pip install streamlit pandas snowflake-snowpark-python
   ```

3. **Configure Snowflake Connection:**
   Since the app uses `snowflake.snowpark.context.get_active_session()`, it expects to be run in an environment where a Snowpark session is actively configured (e.g., inside Snowflake native apps or Streamlit in Snowflake).
   
   *Note: If running entirely locally outside of Snowflake, you will need to modify the connection logic to use `snowflake.connector` and a `.env` file for credentials.*

4. **Run the application:**
   ```bash
   streamlit run streamlit.py
   ```

---

## 🧠 How it Works

1. **User Input:** The user types a question like *"What was Apple's highest price in 2023?"*
2. **SQL Generation:** Cortex AI (`mistral-large`) takes the database schema and the user's question, and generates a raw SQL query.
3. **Execution:** The generated query is executed against the Snowflake database.
4. **Visualization:** If the execution is successful, the data is returned as a Pandas DataFrame and rendered as a table and a chart.
5. **Insights:** Cortex AI parses the top rows of the resulting DataFrame to generate a human-readable, professional financial summary.

---

## 🤝 Contributing

Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](<your-github-repo-url>/issues).

## 📄 License

This project is open-source and available under the [MIT License](LICENSE).

---
*Disclaimer: This application is for educational and research purposes only. It does not constitute financial advice.*
