from unittest.mock import patch
from scripts.monitoring import send_slack_notification, ETLMetrics


def test_etl_metrics():
    metrics = ETLMetrics()
    metrics.records_extracted = 100
    metrics.records_loaded = 95
    metrics.errors = 5

    data = metrics.to_dict()
    assert data['records_extracted'] == 100
    assert 'success_rate_pct' in data


def test_slack_notification():
    with patch('scripts.monitoring.print') as mock_print:
        metrics = ETLMetrics()
        metrics.records_loaded = 150
        send_slack_notification("Test message", metrics.to_dict())
        mock_print.assert_called()

