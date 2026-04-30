# scripts/loaders_bak.py
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

from error_handling import LoadingError


class BaseLoader:
    def load(self, df: pd.DataFrame, table: str) -> int:
        raise NotImplementedError


class UpsertLoader(BaseLoader):
    """Load data using upsert pattern with deduplication."""

    def __init__(self, connection_string: str, primary_keys: list):
        self.engine = create_engine(connection_string)
        self.primary_keys = primary_keys

    def load(self, df: pd.DataFrame, table: str) -> int:
        if df.empty:
            return 0

        try:
            # Get existing records for deduplication
            pk_columns = ','.join(self.primary_keys)
            existing = pd.read_sql(
                f"SELECT {pk_columns} FROM {table}",
                self.engine
            )

            # Filter new/updated records
            mask = ~df[self.primary_keys].apply(tuple, 1).isin(
                existing[self.primary_keys].apply(tuple, 1)
            )
            new_records = df[mask]

            if len(new_records) == 0:
                return 0

            # Use temporary table for upsert
            temp_table = f"temp_{table}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            new_records.to_sql(temp_table, self.engine, if_exists='replace', index=False)

            # Build upsert query (PostgreSQL specific)
            columns = ', '.join(df.columns)
            update_set = ', '.join([f"{c} = EXCLUDED.{c}"
                                    for c in df.columns if c not in self.primary_keys])

            upsert_sql = f"""
                INSERT INTO {table} ({columns})
                SELECT {columns} FROM {temp_table}
                ON CONFLICT ({','.join(self.primary_keys)})
                DO UPDATE SET {update_set}
            """

            with self.engine.begin() as conn:
                conn.execute(text(upsert_sql))
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

            return len(new_records)

        except Exception as e:
            raise LoadingError(f"Failed to load data into {table}: {e}")