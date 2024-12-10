import requests
import pandas as pd
import time
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Configuration
API_URL = "https://api.dotfile.com/v1/cases"
API_KEY = os.getenv("API_KEY")
DATE_FILTER = "2024-12-10T00:00:00Z"  # Replace with last sync date
OUTPUT_CSV = "outputs/cases.csv"
RATE_LIMIT = 150  # 150 requests per minute

def fetch_cases():
    cases = []
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
            cases.extend(data['data'])  # Extract the list of cases from the 'data' field
            pagination = data.get('pagination', {})
            if len(data['data']) < limit or pagination.get('count', 0) <= len(cases):
                break
            page += 1
            time.sleep(60 / RATE_LIMIT)  # Rate limiting
        else:
            print(f"Error: {response.status_code}, {response.text}")
            break

    return cases

def fetch_case_details(case_id):
    url = f"{API_URL}/{case_id}"
    headers = {
        "X-DOTFILE-API-KEY": API_KEY,
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching details for case {case_id}: {response.status_code}, {response.text}")
        return None

def save_to_csv(cases):
    df = pd.json_normalize(cases)  # Flatten nested JSON data
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Data saved to {OUTPUT_CSV}")

def main():
    cases = fetch_cases()
    detailed_cases = []

    for case in cases:
        case_id = case['id']
        details = fetch_case_details(case_id)
        if details:
            detailed_cases.append(details)

    save_to_csv(detailed_cases)

if __name__ == "__main__":
    main()
