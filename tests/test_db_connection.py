# tests/test_db_connection.py
import tests
import pytest
from sqlalchemy import text


def test_database_connection(db_engine):
    """Test that we can connect to the local PostgreSQL database."""
    with db_engine.connect() as conn:
        result = conn.execute(text("SELECT 1 as test"))
        assert result.scalar() == 1
    print("✅ Database connection successful")


def test_tables_exist(db_engine):
    """Test that required tables were created."""
    with db_engine.connect() as conn:
        tables = ['orders', 'customers', 'dead_letter_queue']
        for table in tables:
            result = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table}'
                )
            """))
            assert result.scalar() is True, f"Table {table} does not exist"

    print("✅ All required tables exist")