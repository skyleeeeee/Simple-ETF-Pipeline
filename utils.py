import os
import logging
from datetime import datetime

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

