import requests

api_key = "NDtuqDxsK4HtPQoIoJhAIwqn1bVmzSYZ"
ticker = "AAPL"
url = f"https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={api_key}"

response = requests.get(url)
print(response.status_code)  # Should be 200
print(response.json())        # Should show Apple's data