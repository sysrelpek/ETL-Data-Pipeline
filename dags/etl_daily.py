# dags/etl_daily.py
import os
import sys
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd

# Add project root to sys.path to resolve 'config' and 'scripts' modules
# Airflow typically adds the dags/ folder, but we need the root for scripts/ and config/
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from scripts.extractors import APIExtractor, DatabaseExtractor
from scripts.transformers import DataCleaner, DataValidator, DataAggregator
from scripts.loaders import UpsertLoader
from scripts.utils import save_raw_data, archive_data
from config.settings import ETLConfig
from error_handling import ValidationError, DeadLetterQueue

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def on_failure_callback(context):
    from scripts.monitoring import send_slack_notification
    exception = context.get('exception')
    task_id = context.get('task_instance').task_id
    send_slack_notification(
        message=f"Task {task_id} failed",
        details={"error": str(exception)},
        level="error"
    )

with DAG(
    dag_id='etl_daily',
    default_args=default_args,
    schedule='0 6 * * *',        # Daily at 6:00 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'production'],
    on_failure_callback=on_failure_callback,
) as dag:

    @task(task_id='extract_task')
    def extract_task(job_name: str, **context):
        ds = context['ds']
        run_date = datetime.fromisoformat(ds)
        config = ETLConfig.from_env()
        
        if job_name == 'orders':
            extractor = APIExtractor(f"{config.api_base_url}/orders", config.api_key)
        elif job_name == 'customers':
            extractor = DatabaseExtractor(config.source_postgres, "SELECT * FROM customers WHERE created_at >= ':since'")
        else:
            raise ValueError(f"Unknown job name: {job_name}")
            
        df = extractor.extract(since=run_date)
        save_raw_data(df, job_name, run_date)
        
        # We return the dataframe as a dict for XCom (Airflow handles serialization)
        # Note: Large DataFrames should ideally be passed via S3/File paths, 
        # but for this example we use XCom.
        return df.to_dict(orient='records')

    @task(task_id='transform_task')
    def transform_task(records: list, job_name: str, **context):
        if not records:
            return []
            
        df = pd.DataFrame(records)
        df = DataCleaner().transform(df)
        
        if job_name == 'orders':
            validator = DataValidator({
                'order_id': {'required': True, 'min_value': 0},
                'amount': {'required': True, 'min_value': 0.0},
            })
            try:
                df = validator.transform(df)
            except ValidationError as ve:
                config = ETLConfig.from_env()
                dlq = DeadLetterQueue(config.warehouse_postgres)
                for record in df.to_dict(orient="records")[:100]:
                    dlq.add(record, str(ve), job_name)
                raise
            df = DataAggregator().transform(df)
        elif job_name == 'customers':
            validator = DataValidator({
                'customer_id': {'required': True, 'min_value': 0},
            })
            df = validator.transform(df)
            
        return df.to_dict(orient='records')

    @task(task_id='load_task')
    def load_task(records: list, job_name: str, **context):
        if not records:
            return 0
            
        ds = context['ds']
        run_date = datetime.fromisoformat(ds)
        df = pd.DataFrame(records)
        config = ETLConfig.from_env()
        
        loader = UpsertLoader(
            config.warehouse_postgres,
            primary_keys=["order_id"] if job_name == "orders" else ["customer_id"]
        )
        rows_loaded = loader.load(df, job_name)
        archive_data(job_name, run_date)
        return rows_loaded

    @task(task_id='send_daily_report')
    def send_report(order_count: int, customer_count: int):
        from scripts.monitoring import send_slack_notification
        send_slack_notification(
            f"Daily ETL Pipeline completed. Orders loaded: {order_count}, Customers loaded: {customer_count}",
            level="info"
        )

    # Orders Pipeline
    order_raw = extract_task(job_name='orders')
    order_clean = transform_task(order_raw, job_name='orders')
    order_loaded_count = load_task(order_clean, job_name='orders')

    # Customers Pipeline
    customer_raw = extract_task(job_name='customers')
    customer_clean = transform_task(customer_raw, job_name='customers')
    customer_loaded_count = load_task(customer_clean, job_name='customers')

    # Final Report
    send_report(order_loaded_count, customer_loaded_count)