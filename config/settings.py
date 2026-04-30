# config/settings.py
import os
from dataclasses import dataclass, field
from typing import Dict, Optional
from slack_sdk import WebhookClient
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger("etl_pipeline")

@dataclass
class ETLConfig:
    """Centralized configuration for the ETL pipeline."""

    # Databases
    source_postgres: str = os.getenv('SOURCE_POSTGRES_URL', '')
    warehouse_postgres: str = os.getenv('WAREHOUSE_URL', '')

    # API
    api_base_url: str = os.getenv('API_BASE_URL', '')
    api_key: str = os.getenv('API_KEY', '')

    # Monitoring
    slack_webhook_url: str = os.getenv('SLACK_WEBHOOK_URL', '')

    # Performance
    batch_size: int = int(os.getenv('BATCH_SIZE', 10000))
    max_workers: int = int(os.getenv('MAX_WORKERS', 4))

    # Slack WebClients map
    web_clients: Dict[str, Optional[WebhookClient]] = field(default_factory=dict, init=False)

    def __post_init__(self):
        self._setup_slack_clients()

    def _setup_slack_clients(self):
        if self.slack_webhook_url:
            try:
                self.web_clients['main'] = WebhookClient(self.slack_webhook_url)
                self.web_clients['alerts'] = WebhookClient(self.slack_webhook_url)
                logger.info("Slack WebClients initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize Slack clients: {e}")
                self.web_clients = {}
        else:
            logger.warning("SLACK_WEBHOOK_URL not configured. Notifications disabled.")

    @classmethod
    def from_env(cls) -> 'ETLConfig':
        return cls()

    def get_web_client(self, name: str = 'main') -> Optional[WebhookClient]:
        return self.web_clients.get(name)