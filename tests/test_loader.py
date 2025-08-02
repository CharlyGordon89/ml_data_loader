import os
import pytest
import pandas as pd
from ml_data_loader.loader import DataLoader

TEST_RESOURCES = os.path.join("tests", "resources")

@pytest.mark.parametrize("file_name,file_format,expected_columns", [
    ("test_data.csv", "csv", ["id", "value"]),
    ("test_data.json", "json", ["id", "value"]),
    ("test_data.parquet", "parquet", ["id", "value"]),
])
def test_supported_formats(file_name, file_format, expected_columns):
    path = os.path.join(TEST_RESOURCES, file_name)
    loader = DataLoader(path, file_format=file_format)
    df = loader.load()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert list(df.columns) == expected_columns

def test_infer_format_from_extension():
    path = os.path.join(TEST_RESOURCES, "test_data.csv")
    loader = DataLoader(path)
    df = loader.load()
    assert not df.empty
    assert "id" in df.columns

def test_invalid_path():
    with pytest.raises(FileNotFoundError):
        DataLoader("tests/resources/nonexistent_file.csv")

def test_unsupported_format():
    dummy_path = os.path.join(TEST_RESOURCES, "test.unsupported")
    with open(dummy_path, "w") as f:
        f.write("id,value\n1,a\n2,b\n")

    with pytest.raises(ValueError):
        DataLoader(dummy_path)

    os.remove(dummy_path)

def test_format_mismatch_warning():
    path = os.path.join(TEST_RESOURCES, "test_data.csv")
    with pytest.warns(UserWarning, match="does not match file extension"):
        loader = DataLoader(path, file_format="json")
        with pytest.raises(ValueError):
            loader.load()

def test_not_implemented_cloud_methods():
    path = os.path.join(TEST_RESOURCES, "test_data.csv")
    loader = DataLoader(path)

    with pytest.raises(NotImplementedError):
        loader.load_from_s3()

    with pytest.raises(NotImplementedError):
        loader.load_from_gcs()
