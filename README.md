# Simple-ETF-Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline that ingests user data from a public API, validates and cleans it, then saves the processed data in CSV or Parquet format. It features robust error handling, retry logic, and detailed logging.

---

## Features

- **Data Ingestion:** Fetches user data from the [JSONPlaceholder](https://jsonplaceholder.typicode.com/users) API with retry and exponential backoff.
- **Validation:** Ensures required fields (`user_id`, `email`, `phone`) are present and emails and phone numbers are valid.
- **Data Cleaning:** Normalizes phone numbers and emails, removes invalid or duplicate entries.
- **Flexible Output:** Saves cleaned data as CSV or Parquet files.
- **Logging:** Logs all major steps and metrics to both console and a rotating log file.
- **Command-line interface:** Easily skip ingestion or change output format using CLI arguments.

---

## Project Structure

```

├── data/
│   ├── raw/           # Raw ingested JSON files
│   └── processed/     # Cleaned output files (CSV/Parquet)
├── ingest.py          # Data ingestion and saving logic
├── transform\_validate.py  # Data cleaning, validation, and transformation
├── pipeline.py        # Main pipeline runner with CLI
├── utils.py           # Utility functions (logging, validation, timestamps)
├── logs/
│   └── pipeline.log   # Log files for pipeline runs
└── README.md          # This file

```






## Requirements
```
- Python 3.8+
- Packages:
  - `requests`
  - `pandas`
  - `pyarrow` (if using Parquet output)
```
##Install dependencies with:

```bash
pip install -r requirements.txt
````

---

## Usage

Run the pipeline with default settings (ingest + transform, CSV output):

```
python3 pipeline.py
```

Skip ingestion and only run transform (useful if raw data already exists):

```
python3 pipeline.py --skip-ingest
```

Save output as Parquet instead of CSV:

```
python3 pipeline.py --format parquet
```

---

## Logging

Logs are written both to the console and to `logs/pipeline.log`. The log includes timestamps, log levels, and detailed messages about pipeline progress and errors.

---

## Example Output

```
[INFO] 10 users ingested.
[INFO] Ingestion successful. Data saved to data/raw/users_20250722_004027.json
[INFO] Ingestion completed in 0.17s, saved 10 rows to data/raw/users_20250722_004027.json
[METRICS] Total users ingested           : 10
[METRICS] Users removed (missing fields): 0
[METRICS] Users removed (invalid emails): 0
[METRICS] Users removed (duplicates): 0
[METRICS] Final users saved               : 10
[INFO] Cleaned data saved to data/processed/users_clean_20250722_004027.csv
[INFO] Transformation completed in 0.03s
```

---

## Contributing

Feel free to submit issues or pull requests for improvements.

---

## License

MIT License

```

---

Would you like me to help generate a `requirements.txt` as well?
```
