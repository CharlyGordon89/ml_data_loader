import os
from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd
import warnings

class DataLoader:
    """
    A unified data loader class supporting CSV, JSON, and Parquet formats.

    Attributes:
        path (Path): Path to the data file
        file_format (str): Data format ('csv', 'json', 'parquet')
        options (dict): Extra read options passed to pandas
    """

    def __init__(self, path: str, file_format: Optional[str] = None, **kwargs):
        """
        Initialize the DataLoader with file path and format.

        Args:
            path (str): Full path to the data file.
            file_format (Optional[str]): File format override.
            kwargs: Extra options passed to pandas read method.
        """
        self.path = Path(path)
        self.options = kwargs

        if not self.path.exists():
            raise FileNotFoundError(f"[DataLoader] File not found at path: {self.path}")

        self.file_format = self._infer_file_format(file_format)
        self._validate_file_format()

    def _infer_file_format(self, override_format: Optional[str]) -> str:
        if override_format:
            return override_format.lower()

        ext = self.path.suffix.lower().replace('.', '')
        if ext in {"csv", "json", "parquet"}:
            return ext
        raise ValueError(f"[DataLoader] Unable to infer file format from extension: '{self.path.suffix}'")

    def _validate_file_format(self):
        supported = {"csv", "json", "parquet"}
        if self.file_format not in supported:
            raise ValueError(f"[DataLoader] Unsupported file format: '{self.file_format}'. Supported: {supported}")

        # Optional: warn on mismatch
        ext = self.path.suffix.lower().replace('.', '')
        if ext != self.file_format:
            warnings.warn(
                f"[DataLoader] Provided format '{self.file_format}' does not match file extension '.{ext}'"
            )

    def load(self) -> pd.DataFrame:
        """
        Load the dataset as a pandas DataFrame.
        """
        loader_fn = {
            "csv": self._load_csv,
            "json": self._load_json,
            "parquet": self._load_parquet
        }.get(self.file_format)

        if not loader_fn:
            raise ValueError(f"[DataLoader] No loader found for format '{self.file_format}'")

        return loader_fn()

    def _load_csv(self) -> pd.DataFrame:
        return pd.read_csv(self.path, **self.options)

    def _load_json(self) -> pd.DataFrame:
        return pd.read_json(self.path, **self.options)

    def _load_parquet(self) -> pd.DataFrame:
        return pd.read_parquet(self.path, **self.options)

    # Optional: Add placeholders for future cloud support
    def load_from_s3(self):
        raise NotImplementedError("S3 loading not yet implemented.")

    def load_from_gcs(self):
        raise NotImplementedError("GCS loading not yet implemented.")
