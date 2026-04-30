import pytest
import pandas as pd
from unittest.mock import patch
from error_handling import DeadLetterQueue  # adjust path
from scripts.loaders import UpsertLoader


def test_upsert_loader():
    df = pd.DataFrame({"order_id": [1, 2], "amount": [100.0, 200.0]})

    with patch('sqlalchemy.create_engine'), \
            patch('pandas.read_sql') as mock_read, \
            patch('sqlalchemy.text') as mock_text:
        mock_read.return_value = pd.DataFrame({"order_id": [1]})

        loader = UpsertLoader("postgresql://etluser:etlpassword@127.0.0.1:5432/etl_warehouse", ["order_id"])
        rows = loader.load(df, "orders")

        assert rows == 1  # only the new record


def test_dead_letter_queue():
    with patch('sqlalchemy.create_engine'), patch('sqlalchemy.text'):
        dlq = DeadLetterQueue("postgresql://etluser:etlpassword@127.0.0.1:5432/etl_warehouse")
        dlq.add({"order_id": 1}, "test error", "orders")
        # Add assertion via mock if needed