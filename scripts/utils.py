# scripts/utils_bak.py
import pandas as pd
from datetime import datetime
from pathlib import Path


def save_raw_data(df: pd.DataFrame, source: str, run_date: datetime):
    """Save raw extracted data as parquet."""
    date_str = run_date.strftime("%Y-%m-%d")
    path = Path(f"data/raw/{source}_{date_str}.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def archive_data(source: str, run_date: datetime):
    """Move processed raw data to archive folder."""
    date_str = run_date.strftime("%Y-%m-%d")
    raw_path = Path(f"data/raw/{source}_{date_str}.parquet")

    if raw_path.exists():
        archive_path = Path(f"data/archive/{source}_{date_str}.parquet")
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.rename(archive_path)