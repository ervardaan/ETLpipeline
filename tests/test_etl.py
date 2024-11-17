import pytest
import pandas as pd
from etl_pipeline import extract_data, transform_data, load_data

# Sample data for testing
@pytest.fixture
def sample_csv_data():
    return pd.DataFrame({
        "experiment_id": [1, 2],
        "user_id": [1, 1],
        "experiment_compound_ids": ["1;2", "2;3"],
        "experiment_run_time": [10, 15],
    })

@pytest.fixture
def transformed_data():
    return pd.DataFrame({
        "user_id": [1],
        "total_experiments": [2],
        "average_experiment_runtime": [12.5],
        "most_common_compound": ["2"]
    })

# Test extraction
def test_extract_data(sample_csv_data, mocker):
    mocker.patch("pandas.read_csv", return_value=sample_csv_data)
    data = extract_data("dummy_path.csv")
    assert not data.empty
    assert list(data.columns) == ["experiment_id", "user_id", "experiment_compound_ids", "experiment_run_time"]

# Test transformation
def test_transform_data(sample_csv_data, transformed_data):
    result = transform_data(sample_csv_data)
    pd.testing.assert_frame_equal(result, transformed_data)

# Test loading (mock database connection)
def test_load_data(mocker, transformed_data):
    mock_conn = mocker.MagicMock()
    mocker.patch("psycopg2.connect", return_value=mock_conn)
    load_data(transformed_data, "user_summary", mock_conn)
    mock_conn.cursor().execute.assert_called()  # Check if queries were executed
