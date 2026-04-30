# tests/test_transformers_old.py
import pandas as pd
import pytest

from scripts.transformers import DataCleaner, DataValidator, DataAggregator, ValidationError

def test_data_cleaner():
    df = pd.DataFrame({
        'status': [None, ' Active ', ''],
        'amount': [None, 100, -5],
        'name': ['  John  ', 'Alice', ' Bob ']
    })

    result = DataCleaner().transform(df)

    assert result['status'].iloc[0] == 'unknown'
    assert result['status'].iloc[1] == 'active'
    assert result['amount'].iloc[0] == 0.0
    assert result['name'].iloc[0] == 'john'


def test_data_validator_success():
    df = pd.DataFrame({
        'order_id': [1, 2, 3],
        'amount': [10, 20, 30]
    })

    validator = DataValidator({
        'order_id': {'required': True},
        'amount': {'min_value': 0}
    })

    result = validator.transform(df)
    assert len(result) == 3


def test_data_validator_failure():
    df = pd.DataFrame({
        'order_id': [1, None],
        'amount': [10, -5]
    })

    validator = DataValidator({
        'order_id': {'required': True},
        'amount': {'min_value': 0}
    })

    with pytest.raises(ValidationError):
        validator.transform(df)


def test_data_aggregator():
    df = pd.DataFrame({
        'customer_id': [1, 1, 2],
        'date': ['2024-01-15'] * 3,
        'order_id': [101, 102, 103],
        'amount': [50, 60, 70],
        'items': [2, 3, 1]
    })

    result = DataAggregator().transform(df)

    assert len(result) == 2
    assert result[result['customer_id'] == 1]['order_id'].iloc[0] == 2