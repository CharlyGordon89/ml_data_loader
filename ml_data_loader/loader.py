"""
Reusable data loader for ML pipelines.
Loads CSV, JSON, Parquet, or SQL from standardized data folders:
raw/, interim/, and processed/
"""

import os
import pandas as pd

SUPPORTED_FORMATS = (".csv", ".json", ".parquet")


def load_data(path: str, file_format: str = "csv", **kwargs) -> pd.DataFrame:
    """
    Load data from a given path and format.

    Args:
        path (str): Full path to the data file
        file_format (str): Type of file ('csv', 'json', 'parquet')
        kwargs: Extra options passed to pandas read method

    Returns:
        pd.DataFrame: Loaded dataset
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    
    file_format = file_format.lower()

    if file_format == "csv":
        return pd.read_csv(path, **kwargs)
    elif file_format == "json":
        return pd.read_json(path, **kwargs)
    elif file_format == "parquet":
        return pd.read_parquet(path, **kwargs)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
