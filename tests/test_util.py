# tests/test_utils.py
from scripts.utils import save_raw_data, archive_data
from datetime import datetime
import pandas as pd
from pathlib import Path

def test_save_raw_data():
    df = pd.DataFrame({'col': [1, 2, 3]})
    run_date = datetime(2024, 1, 15)

    save_raw_data(df, "test_source", run_date)

    file_path = Path("data/raw/test_source_2024-01-15.parquet")
    assert file_path.exists()
    file_path.unlink()  # cleanup