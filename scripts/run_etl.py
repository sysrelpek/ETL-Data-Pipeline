"""
ETL Data Pipeline - Main entry
Works on both local development and production server.
"""

import argparse
import sys
import os
from datetime import datetime

# Add project root to sys.path to resolve 'config' and 'scripts' modules
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from concurrent.futures import ThreadPoolExecutor, as_completed

from config.settings import ETLConfig
from config.logging import setup_logging
from scripts.monitoring import ETLMetrics, send_slack_notification
from scripts.extractors import APIExtractor, DatabaseExtractor, MockAPIExtractor
from scripts.transformers import DataCleaner, DataValidator, DataAggregator
from scripts.utils import save_raw_data, archive_data
from scripts.loaders import UpsertLoader
from error_handling import DeadLetterQueue, ValidationError


logger = setup_logging()

def run_single_job(job_name: str, run_date: datetime, config: ETLConfig, mock: bool = False):
    """ Run one ETL job - clean, robust and environment agnostic"""
    metrics = ETLMetrics()
    dlq = DeadLetterQueue(config.warehouse_postgres)

    try:
        logger.info(f"Starting ETL job: {job_name} - {run_date.strftime('%Y-%m-%d')} {'(MOCK)' if mock else ''}")

        # Extract
        if mock:
            mock_file = "tests/test_data/sample_orders.csv" if job_name == 'orders' else "tests/test_data/sample_customers.csv"
            extractor = MockAPIExtractor(mock_file)
            df = extractor.extract(since=run_date)
        elif job_name == 'orders':
            extractor = APIExtractor(f"{config.api_base_url}/orders", config.api_key)
            df = extractor.extract(since=run_date)
        elif job_name == 'customers':
            extractor = DatabaseExtractor(config.source_postgres, "SELECT * FROM customers WHERE created_at >= ':since'")
            df = extractor.extract(since=run_date)
        else:
            raise ValueError(f"Unknown job name: {job_name}")

        metrics.records_extracted = len(df)
        save_raw_data(df, job_name, run_date)

        # Transform
        df = DataCleaner().transform(df)
        metrics.records_transformed = len(df)

        if job_name == 'orders':
            validator = DataValidator({
                'order_id': {'required': True, 'min_value': 0},
                'amount': {'required': True, 'min_value': 0.0},
            })
            df = validator.transform(df)
            df = DataAggregator().transform(df)
        elif job_name == 'customers':
            validator = DataValidator({
                'customer_id': {'required': True, 'min_value': 0},
            })
            df = validator.transform(df)

        # Load
        loader = UpsertLoader(
            config.warehouse_postgres,
            primary_keys=["order_id"] if job_name == "orders" else ["customer_id"]
        )
        rows_loaded = loader.load(df, job_name)
        metrics.records_loaded = rows_loaded

        archive_data(job_name, run_date)
        logger.info(f"ETL job completed: {job_name} Loaded: {rows_loaded} rows")
        send_slack_notification(f"ETL job {job_name} completed successfully", metrics.to_dict(), "info")
        return metrics

    except ValidationError as ve:
        logger.warning(f"validation failed for {job_name}: {ve}")
        send_slack_notification(f"Validation error in {job_name}", {"error": str(ve)}, "warning")
        for record in df.to_dict(orient="records")[:100]:
            dlq.add(record, str(ve), job_name)
        raise

    except Exception as e:
        metrics.errors += 1
        logger.error(f"ETL job {job_name} failed", exc_info=True)
        send_slack_notification(f"ETL job {job_name} failed", {"error": str(e)}, "error")
        raise

def main():
    parser = argparse.ArgumentParser(description="Run ETL Data Pipeline")
    parser.add_argument("--job", choices=["orders", "customers"], help="Run specific job")
    parser.add_argument("--since", type=str, help="Run date (YYYY-MM-DD)")
    parser.add_argument("--all", action="store_true", help="Run all jobs in parallel")
    parser.add_argument("--mock", action="store_true", help="Use mock data instead of API/DB")
    args = parser.parse_args()

    run_date = datetime.fromisoformat(args.since) if args.since else datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    config = ETLConfig.from_env()

    # Automatically enable mock mode if API URL is a placeholder and not in production
    if not args.mock and "api.example.com" in config.api_base_url:
        logger.info("Detected placeholder API URL. Enabling --mock mode automatically.")
        args.mock = True

    if args.all:
        logger.info("Running all jobs in parallel")
        with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
            futures = {executor.submit(run_single_job, job, run_date, config, args.mock): job
                       for job in ["orders", "customers"]}
            for future in as_completed(futures):
                job = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"Job {job} failed: {exc}")
    elif args.job:
        run_single_job(args.job, run_date, config, args.mock)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()