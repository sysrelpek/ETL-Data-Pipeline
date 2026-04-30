# scripts/extractors_bak.py
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine

from error_handling import ExtractionError


class BaseExtractor:
    """Base class for all extractors."""

    def extract(self, since: datetime) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement extract()")

    def get_source_name(self) -> str:
        return self.__class__.__name__


class APIExtractor(BaseExtractor):
    """Extract data from REST APIs with pagination support and retries."""

    def __init__(self, endpoint: str, api_key: str, max_retries: int = 3):
        self.endpoint = endpoint
        self.api_key = api_key
        self.session = self._status_session(max_retries)

    def _status_session(self, max_retries: int) -> requests.Session:
        """Create a requests session with retry strategy."""
        session = requests.Session()
        retry = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def extract(self, since: datetime) -> pd.DataFrame:
        all_records = []
        page = 1

        try:
            while True:
                response = self.session.get(
                    self.endpoint,
                    params={'since': since.isoformat(), 'page': page},
                    headers={'Authorization': f'Bearer {self.api_key}'},
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()

                if not data.get('records'):
                    break

                all_records.extend(data['records'])
                page += 1

                if page > data.get('total_pages', 1):
                    break

            return pd.DataFrame(all_records)

        except requests.exceptions.ConnectionError as ce:
            error_msg = f"Connection failed for {self.endpoint}. Check your internet connection or API URL."
            if "example.com" in self.endpoint:
                error_msg += " (Note: 'example.com' is a placeholder. Use --mock flag for testing with sample data.)"
            raise ExtractionError(f"{error_msg} Details: {ce}")
        except Exception as e:
            raise ExtractionError(f"API extraction failed from {self.endpoint}: {e}")


class MockAPIExtractor(BaseExtractor):
    """Mock extractor that reads from local files for development and testing."""

    def __init__(self, mock_data_path: str):
        self.mock_data_path = mock_data_path

    def extract(self, since: datetime) -> pd.DataFrame:
        try:
            if self.mock_data_path.endswith('.csv'):
                df = pd.read_csv(self.mock_data_path)
            elif self.mock_data_path.endswith('.json'):
                df = pd.read_json(self.mock_data_path)
            elif self.mock_data_path.endswith('.parquet'):
                df = pd.read_parquet(self.mock_data_path)
            else:
                raise ValueError(f"Unsupported mock data format: {self.mock_data_path}")

            # Simple filtering by date if a date column exists
            for date_col in ['date', 'created_at']:
                if date_col in df.columns:
                    df[date_col] = pd.to_datetime(df[date_col])
                    df = df[df[date_col] >= pd.to_datetime(since)]
                    break

            return df
        except Exception as e:
            raise ExtractionError(f"Mock extraction failed from {self.mock_data_path}: {e}")


class DatabaseExtractor(BaseExtractor):
    """Extract data from PostgreSQL with incremental loading."""

    def __init__(self, connection_string: str, query: str):
        self.engine = create_engine(connection_string)
        self.query = query

    def extract(self, since: datetime) -> pd.DataFrame:
        try:
            query = self.query.replace(':since', since.isoformat())
            return pd.read_sql(query, self.engine)
        except Exception as e:
            raise ExtractionError(f"Database extraction failed: {e}")