import os
import json
import re
import pandas as pd
from datetime import datetime

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

