# config/logging.py
import logging
import sys
from pathlib import Path

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Setup structured logging for the ETL pipeline."""
    logger = logging.getLogger("etl_pipeline")
    logger.setLevel(getattr(logging, log_level.upper()))

    # Remove existing handlers
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    logger.addHandler(console_handler)

    # File handler
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(log_dir / "etl_pipeline.log")
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s'
    ))
    logger.addHandler(file_handler)

    return logger