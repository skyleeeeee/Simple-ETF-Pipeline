import argparse
import time
import sys
import logging

from ingest import get_users, save_users
import transform_validate as tv
from utils import setup_logging  # Your utility module with setup_logging()

def print_report(report):
    print("\n=== Pipeline Execution Report ===")
    for step, info in report.items():
        print(f"\nStep: {step.capitalize()}")
        print(f"  Status     : {info['status']}")
        if info['duration'] is not None:
            print(f"  Duration   : {info['duration']:.2f} seconds")
        if info['output_file']:
            print(f"  Output file: {info['output_file']}")
        if info['rows'] is not None:
            print(f"  Rows       : {info['rows']}")
    print("=================================")

def main(skip_ingest, output_format):
    # Initialize logging (logs both to file and console)
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting ETL pipeline")
    report = {}

    # Ingestion
    if skip_ingest:
        logger.info("Skipping ingestion step.")
        report['ingest'] = {
            'status': 'skipped',
            'duration': 0,
            'output_file': None,
            'rows': None
        }
    else:
        start = time.perf_counter()
        users = get_users()
        if users is None:
            logger.error("Ingestion failed.")
            report['ingest'] = {'status': 'failed', 'duration': None, 'output_file': None, 'rows': None}
            print_report(report)
            sys.exit(1)
        save_path = save_users(users)
        duration = time.perf_counter() - start
        rows = len(users)
        report['ingest'] = {'status': 'successful', 'duration': duration, 'output_file': save_path, 'rows': rows}
        logger.info(f"Ingestion completed in {duration:.2f}s, saved {rows} rows to {save_path}")

    # Transformation & Validation
    start = time.perf_counter()
    try:
        output_path, final_count = tv.transform_and_validate(output_format)
        duration = time.perf_counter() - start
        report['transform'] = {
            'status': 'successful',
            'duration': duration,
            'output_file': output_path,
            'rows': final_count
        }
        logger.info(f"Transformation completed in {duration:.2f}s")

    except Exception as e:
        duration = time.perf_counter() - start
        logger.error(f"Transformation failed: {e}")
        report['transform'] = {
            'status': 'failed',
            'duration': duration,
            'output_file': None,
            'rows': None
        }
        print_report(report)
        sys.exit(1)

    print_report(report)
    logger.info("ETL pipeline finished successfully")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL pipeline for users data")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip the ingestion step")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Output format for transformed data")
    args = parser.parse_args()

    main(args.skip_ingest, args.format)

