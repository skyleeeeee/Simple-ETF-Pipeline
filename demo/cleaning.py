import requests
import os
import json
import time
import logging
from datetime import datetime
import pandas as pd
import re
import sys


def current_timestamp(fmt="%Y%m%d_%H%M%S"):
    return datetime.now().strftime(fmt)

def validate_user_schema(data, required_fields=None):
    if required_fields is None:
        required_fields = ['user_id', 'email', 'phone']

    for idx, record in enumerate(data):
        for field in required_fields:
            if field not in record:
                raise ValueError(f"Record at index {idx} missing required field '{field}'")

def format_filename(prefix, ext, timestamp=None):
    if timestamp is None:
        timestamp = current_timestamp()
    filename = f"{prefix}_{timestamp}.{ext}"
    return filename

def setup_folders(*folders):
    for folder in folders:
        os.makedirs(folder, exist_ok=True)

def setup_logging(log_dir="logs", log_file="pipeline.log"):
    setup_folders(log_dir)
    log_path = os.path.join(log_dir, log_file)

    logging.basicConfig(
        filename=log_path,
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    # Also log to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

    return log_path



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

def load_most_recent_raw():
    raw_dir = "data/raw"
    files = [f for f in os.listdir(raw_dir) if f.startswith("users_") and f.endswith(".json")]
    if not files:
        raise FileNotFoundError("No raw user files found in data/raw")

    files.sort(reverse=True)
    latest_file = files[0]
    path = os.path.join(raw_dir, latest_file)

    with open(path, "r") as f:
        data = json.load(f)
    return data, latest_file

def clean_phone(phone):
    if not isinstance(phone, str):
        return ""
    return re.sub(r"\D", "", phone)

def validate_email(email):
    if not isinstance(email, str):
        return False
    pattern = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
    return bool(pattern.match(email))

def transform_and_validate(output_format="csv"):
    data, raw_filename = load_most_recent_raw()
    total_ingested = len(data)

    df = pd.DataFrame(data)

    required_fields = ['user_id', 'email', 'phone']
    before_drop = len(df)
    df = df.dropna(subset=required_fields)
    removed_missing = before_drop - len(df)

    df['phone'] = df['phone'].apply(clean_phone)
    df['email'] = df['email'].str.lower()

    valid_email_mask = df['email'].apply(validate_email)
    removed_bad_emails = (~valid_email_mask).sum()

    df = df[valid_email_mask]
    df = df[df['phone'] != ""]

    before_dedup = len(df)

    if df['user_id'].duplicated().any():
        duplicates = df[df['user_id'].duplicated()]['user_id'].tolist()
        print(f"[WARNING] Duplicate user IDs found and removed: {duplicates}")
        df = df.drop_duplicates(subset=['user_id'], keep='first')

    removed_duplicates = before_dedup - len(df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "data/processed"
    os.makedirs(output_dir, exist_ok=True)

    if output_format == "csv":
        output_path = os.path.join(output_dir, f"users_clean_{timestamp}.csv")
        df.to_csv(output_path, index=False)
    else:
        output_path = os.path.join(output_dir, f"users_clean_{timestamp}.parquet")
        df.to_parquet(output_path, index=False)

    final_count = len(df)

    print(f"[METRICS] Total users ingested           : {total_ingested}")
    print(f"[METRICS] Users removed (missing fields): {removed_missing}")
    print(f"[METRICS] Users removed (invalid emails): {removed_bad_emails}")
    print(f"[METRICS] Users removed (duplicates): {removed_duplicates}")
    print(f"[METRICS] Final users saved               : {final_count}")
    print(f"[INFO] Cleaned data saved to {output_path}")

    return output_path, final_count


