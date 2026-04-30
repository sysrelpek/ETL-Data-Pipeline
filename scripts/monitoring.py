import json
from datetime import datetime
import logging
from typing import Optional, Dict


from config.settings import ETLConfig

logger = logging.getLogger("etl_pipeline")

class ETLMetrics:
    def __init__(self):
        self.start_time = datetime.utcnow()
        self.records_extracted = 0
        self.records_transformed = 0
        self.records_loaded = 0
        self.errors = 0

    def to_dict(self) -> Dict:
        duration = (datetime.utcnow() - self.start_time).total_seconds()
        return {
            "duration_seconds": round(duration, 2),
            "records_extracted": self.records_extracted,
            "records_transformed": self.records_transformed,
            "records_loaded": self.records_loaded,
            "errors": self.errors,
            "success_rate_pct": round(100 * (1 - self.errors / max(1, self.records_extracted)), 2)
        }


def send_slack_notification(message: str, metrics: Optional[Dict] = None, level: str = "info"):
    """ Sending Slack notification (local test version)"""
    config = ETLConfig.from_env()
    print(f" Slack Notification: {message}, {metrics}")

