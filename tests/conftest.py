# tests/conftest.py
import pytest
import tests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os

from config.settings import ETLConfig


@pytest.fixture(scope="session")
def test_config():
    """Provide test configuration."""
    config = ETLConfig.from_env()
    # Override for testing if needed
    return config


@pytest.fixture(scope="session")
def db_engine(test_config):
    """Create database engine and ensure tables exist."""
    engine = create_engine(test_config.warehouse_postgres)

    # Create necessary tables for testing
    init_test_database(engine)

    return engine


def init_test_database(engine):
    """Initialize test database tables if they don't exist."""
    with engine.begin() as conn:
        # Create orders table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id BIGINT PRIMARY KEY,
                customer_id BIGINT,
                amount NUMERIC(12,2),
                status VARCHAR(50),
                date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # Create customers table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id BIGINT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # Create dead_letter_queue table (already handled in DeadLetterQueue, but ensure it exists)
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

    print("✅ Test database tables initialized successfully")


@pytest.fixture
def load_test_data(db_engine):
    """Load test data from CSV into the database."""
    csv_path = "tests/test_data/sample_orders.csv"
    df = pd.read_csv(csv_path)
    
    # Load into 'orders' table
    # Note: we use if_exists='append' because table is already created in init_test_database
    df.to_sql('orders', con=db_engine, if_exists='append', index=False)
    
    return df


@pytest.fixture
def sample_orders_df():
    """Provide sample orders data for testing."""
    return pd.DataFrame({
        'order_id': [1001, 1002, 1003],
        'customer_id': [501, 501, 502],
        'amount': [150.50, 75.25, 200.00],
        'status': ['completed', 'pending', 'completed'],
        'date': ['2024-01-15', '2024-01-15', '2024-01-15'],
        'created_at': [datetime(2024, 1, 15)] * 3
    })


@pytest.fixture
def sample_customers_df():
    """Provide sample customers data for testing."""
    return pd.DataFrame({
        'customer_id': [501, 502],
        'name': ['john doe', 'alice smith'],
        'email': ['john@example.com', 'alice@example.com']
    })