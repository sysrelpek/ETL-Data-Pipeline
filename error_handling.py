# error_handling.py
import json
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import logging

logger = logging.getLogger("etl_pipeline")


class ETLError(Exception):
    """Base ETL exception."""
    pass


class ExtractionError(ETLError):
    pass


class TransformationError(ETLError):
    pass


class LoadingError(ETLError):
    pass


class ValidationError(ETLError):
    """Raised when data validation fails."""
    pass


class DeadLetterQueue:
    """Store failed records for later analysis or retry."""

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Create DLQ table if it doesn't exist."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dead_letter_queue (
                    id SERIAL PRIMARY KEY,
                    record JSONB NOT NULL,
                    error TEXT NOT NULL,
                    pipeline VARCHAR(100) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed BOOLEAN DEFAULT FALSE,
                    processed_at TIMESTAMP
                )
            """))

    def add(self, record: dict, error: str, pipeline: str):
        """Add failed record to dead letter queue."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO dead_letter_queue (record, error, pipeline, created_at)
                    VALUES (:record, :error, :pipeline, :created_at)
                """), {
                    'record': json.dumps(record),
                    'error': str(error),
                    'pipeline': pipeline,
                    'created_at': datetime.utcnow()
                })
            logger.info(f"Record added to Dead Letter Queue for pipeline: {pipeline}")
        except Exception as e:
            logger.error(f"Failed to write to DeadLetterQueue: {e}")

    def get_pending(self, limit: int = 100) -> pd.DataFrame:
        return pd.read_sql(
            f"SELECT * FROM dead_letter_queue WHERE processed = false LIMIT {limit}",
            self.engine
        )