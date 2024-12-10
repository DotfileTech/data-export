import requests
import pandas as pd
import time
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Configuration
API_URL = "https://api.dotfile.com/v1/checks"
API_KEY = os.getenv("API_KEY")
DATE_FILTER = "2024-12-10T00:00:00Z"  # Replace with last sync date
OUTPUT_CSV = "outputs/checks.csv"
RATE_LIMIT = 150  # 150 requests per minute

def fetch_checks():
    checks = []
    page = 1
    limit = 100  # Maximum limit per page

    while True:
        params = {
            "last_activity_at.gte": DATE_FILTER,
            "page": page,
            "limit": limit,
        }
        headers = {
            "X-DOTFILE-API-KEY": API_KEY,
            "Content-Type": "application/json"
        }

        response = requests.get(API_URL, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            checks.extend(data['data'])  # Extract the list of checks from the 'data' field
            pagination = data.get('pagination', {})
            if len(data['data']) < limit or pagination.get('count', 0) <= len(checks):
                break
            page += 1
            time.sleep(60 / RATE_LIMIT)  # Rate limiting
        else:
            print(f"Error: {response.status_code}, {response.text}")
            break

    return checks

def fetch_check_details(check_type, check_id):
    url = f"{API_URL}/{check_type}/{check_id}"
    headers = {
        "X-DOTFILE-API-KEY": API_KEY,
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching details for check {check_id}: {response.status_code}, {response.text}")
        return None

def save_to_csv(checks):
    df = pd.json_normalize(checks)  # Flatten nested JSON data
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Data saved to {OUTPUT_CSV}")

def main():
    checks = fetch_checks()
    detailed_checks = []

    for check in checks:
        check_id = check['id']
        check_type = check['type']
        details = fetch_check_details(check_type, check_id)
        if details:
            detailed_checks.append(details)

    save_to_csv(detailed_checks)

if __name__ == "__main__":
    main()
