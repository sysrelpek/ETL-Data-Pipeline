# scripts/transformers_bak.py
import pandas as pd

from error_handling import ValidationError


class BaseTransformer:
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError


class DataCleaner(BaseTransformer):
    """Clean and standardize data."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        # Remove duplicates
        df = df.drop_duplicates()

        # Handle missing values
        df = df.fillna({'status': 'unknown', 'amount': 0.0})

        # Standardize string columns
        string_cols = df.select_dtypes(include=['object', 'string']).columns
        for col in string_cols:
            df[col] = df[col].astype(str).str.strip().str.lower()

        return df


class DataValidator(BaseTransformer):
    """Validate data quality based on rules."""

    def __init__(self, rules: dict):
        self.rules = rules

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        errors = []

        for col, rule in self.rules.items():
            if col not in df.columns:
                errors.append(f"Missing required column: {col}")
                continue

            if rule.get('required') and df[col].isna().any():
                errors.append(f"Null values found in required column: {col}")

            if rule.get('min_value') is not None and df[col].min() < rule['min_value']:
                errors.append(f"Values below minimum in column {col}")

            if rule.get('pattern') and not df[col].astype(str).str.match(rule['pattern']).all():
                errors.append(f"Pattern mismatch in column {col}")

        if errors:
            raise ValidationError(f"Data validation failed: {errors}")

        return df


class DataAggregator(BaseTransformer):
    """Aggregate data for analytics."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or 'customer_id' not in df.columns:
            return df

        # Ensure date column exists for grouping
        if 'date' not in df.columns and 'created_at' in df.columns:
            df['date'] = pd.to_datetime(df['created_at']).dt.date

        if 'date' not in df.columns:
            return df

        # Define aggregation map based on available columns
        agg_map = {
            'order_id': 'count',
            'amount': 'sum'
        }
        if 'items' in df.columns:
            agg_map['items'] = 'sum'

        return df.groupby(['customer_id', 'date']).agg(agg_map).reset_index()