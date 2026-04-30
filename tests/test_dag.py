# tests/test_dag.py
import pytest
from airflow.models import DagBag
from datetime import datetime
from unittest.mock import patch, MagicMock

@pytest.fixture
def dagbag():
    return DagBag(dag_folder='dags/', include_examples=False)

def test_dag_loaded(dagbag):
    """Test that the DAG is loaded correctly."""
    dag = dagbag.get_dag(dag_id='etl_daily')
    assert dagbag.import_errors == {}, f"Import errors: {dagbag.import_errors}"
    assert dag is not None
    # We now have multiple tasks per pipeline (extract, transform, load) x 2 + report
    # Plus possibly some helper tasks or expanded tasks
    assert len(dag.tasks) >= 7

def test_dag_structure(dagbag):
    """Test the structure and dependencies of the DAG."""
    dag = dagbag.get_dag(dag_id='etl_daily')
    
    # Check if new tasks exist
    assert dag.has_task('extract_task')
    assert dag.has_task('transform_task')
    assert dag.has_task('load_task')
    assert dag.has_task('send_daily_report')

@patch('scripts.monitoring.send_slack_notification')
def test_send_report_task(mock_slack, dagbag):
    """Test the send_report task execution."""
    dag = dagbag.get_dag(dag_id='etl_daily')
    task = dag.get_task('send_daily_report')
    
    # Test the callable with dummy data
    task.python_callable(order_count=10, customer_count=5)
    
    mock_slack.assert_called_once()
    args, kwargs = mock_slack.call_args
    assert "Daily ETL Pipeline completed" in args[0]
    assert "Orders loaded: 10" in args[0]
    assert "Customers loaded: 5" in args[0]
    assert kwargs['level'] == "info"
