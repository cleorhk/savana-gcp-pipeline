import requests
import os
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

# Load environment variables
load_dotenv()

# Define API URLs
API_ENDPOINTS = {
    "users": "https://dummyjson.com/users",
    "products": "https://dummyjson.com/products",
    "carts": "https://dummyjson.com/carts"
}

def fetch_data(api_name, url):
    """Fetch data from an API and return the JSON response."""
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Successfully fetched data from {api_name} API.")
        return response.json()
    else:
        print(f"Failed to fetch data from {api_name} API: {response.status_code}, {response.text}")
        return {}

def save_data_to_file(data, filename):
    """Save data to a JSON file."""
    Path("data").mkdir(exist_ok=True)
    filepath = f"data/{filename}"
    with open(filepath, "w") as file:
        import json
        json.dump(data, file, indent=4)
    print(f"Data saved to {filepath}")

def fetch_all_data():
    """Fetch data from all APIs and save them to JSON files."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Current timestamp
    for api_name, url in API_ENDPOINTS.items():
        data = fetch_data(api_name, url)
        if data:
            # Add a timestamp for traceability
            data["timestamp"] = timestamp
            save_data_to_file(data, f"{api_name}.json")

if __name__ == "__main__":
    print("Starting data extraction process...")
    fetch_all_data()
    print("Data extraction process completed.")
