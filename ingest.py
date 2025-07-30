import requests
import os
import json
import time
from utils import current_timestamp, validate_user_schema, setup_folders

def get_users(max_retries=5, backoff_factor=1):
    url = "https://jsonplaceholder.typicode.com/users"

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()

            for record in data:
                if 'id' in record:
                    record['user_id'] = record.pop('id')

            validate_user_schema(data)

            return data

        except (requests.RequestException, ValueError) as e:
            if attempt == max_retries:
                print(f"[ERROR] Ingestion failed after {max_retries} attempts: {e}")
                return None
            wait = backoff_factor * 2 ** (attempt - 1)
            print(f"[WARNING] Attempt {attempt} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)

def save_users(data):
    if not data:
        print("[ERROR] No data to save.")
        return None

    setup_folders("data/raw")
    timestamp = current_timestamp()
    path = f"data/raw/users_{timestamp}.json"

    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=4)
        print(f"[INFO] {len(data)} users ingested.")
        print(f"[INFO] Ingestion successful. Data saved to {path}")
        return path
    except Exception as e:
        print(f"[ERROR] Failed to save data: {e}")
        return None

