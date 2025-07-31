from ml_data_loader.loader import load_data
import os

def test_load_csv():
    test_path = os.path.join("tests", "resources", "test_data.csv")
    df = load_data(test_path, file_format="csv")
    assert not df.empty
    assert list(df.columns) == ["id", "value"]
