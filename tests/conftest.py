import pytest
import os
import pandas as pd

TEST_RESOURCES = os.path.join("tests", "resources")

@pytest.fixture(scope="session", autouse=True)
def setup_test_files():
    """Create sample CSV, JSON, and Parquet test files before tests run."""
    os.makedirs(TEST_RESOURCES, exist_ok=True)
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})

    df.to_csv(os.path.join(TEST_RESOURCES, "test_data.csv"), index=False)
    df.to_json(os.path.join(TEST_RESOURCES, "test_data.json"), orient="records", lines=False)
    df.to_parquet(os.path.join(TEST_RESOURCES, "test_data.parquet"))

    yield  # tests run here

    # Clean up after tests
    for file in ["test_data.csv", "test_data.json", "test_data.parquet"]:
        os.remove(os.path.join(TEST_RESOURCES, file))
