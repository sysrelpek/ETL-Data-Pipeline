# tests/test_config.py
import tests
from config.settings import ETLConfig

def test_etl_config_from_env():
    config = ETLConfig.from_env()
    assert isinstance(config.max_workers, int)
    assert config.batch_size > 0
    assert config.warehouse_postgres.startswith("postgresql://")