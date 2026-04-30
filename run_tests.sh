#!/bin/bash
# ================================================================
# run_tests.sh - Run full test suite with database initialization
# ================================================================

set -e

echo "=== ETL Pipeline Test Runner ==="

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "Virtual environment activated."
fi

# Install test dependencies if needed
pip install pytest pytest-mock pyarrow > /dev/null

echo "1. Initializing test database..."
python3 -m tests.init_db

echo "2. Running all tests..."
pytest tests/ -v --tb=short

echo "3. Running sample ETL job..."
PYTHONPATH=. python3 scripts/run_etl.py --job orders --since 2024-01-15

echo ""
echo "✅ All tests completed successfully!"


