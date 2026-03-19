import requests

key = "NDtuqDxsK4HtPQoIoJhAIwqn1bVmzSYZ"

# Test stable endpoint (new API format)
urls = {
    "stable/quote": f"https://financialmodelingprep.com/stable/quote?symbol=AAPL&apikey={key}",
    "stable/historical": f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&apikey={key}",
    "v3/quote": f"https://financialmodelingprep.com/api/v3/quote/AAPL?apikey={key}",
    "v3/historical": f"https://financialmodelingprep.com/api/v3/historical-price-full/AAPL?apikey={key}",
}

for name, url in urls.items():
    try:
        resp = requests.get(url, timeout=10)
        print(f"{name}: {resp.status_code} - {resp.text[:120]}")
    except Exception as e:
        print(f"{name}: ERROR - {e}")
