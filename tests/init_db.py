# tests/init_db.py
"""
Run this script to manually initialize the test database tables.
"""
from sqlalchemy import create_engine, text

from config.settings import ETLConfig

def init_database():
    config = ETLConfig.from_env()
    engine = create_engine(config.warehouse_postgres)

    with engine.begin() as conn:
        print("Creating orders table...")
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

        print("Creating customers table...")
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id BIGINT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        print("Creating dead_letter_queue table...")
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

    print("✅ All test database tables have been initialized successfully!")

if __name__ == "__main__":
    init_database()