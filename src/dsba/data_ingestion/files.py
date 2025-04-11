from io import StringIO
from pathlib import Path
from typing import Any
import requests
import pandas as pd

import os
from dsba.simple_cache import cache_to_disk


def load_csv_from_path(filepath: str | Path) -> pd.DataFrame:
    """
    Loads a CSV file on the local filesystem into a pandas DataFrame
    Since it loads it all in memory, it is only suitable for datasets small enough to fit in memory
    """
    return pd.read_csv(filepath)


def load_csv_from_url(url: str) -> pd.DataFrame:
    response = requests.get(url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def write_csv_to_path(df: pd.DataFrame, filepath: str | Path) -> None:
    df.to_csv(filepath, index=False)



@cache_to_disk(cache_file=os.path.join("cache", "ingested_data.pkl"))
def ingest_data(filepath: str, type: str) -> pd.DataFrame:
    """
    Loads a CSV file on the local filesystem into a pandas DataFrame, while caching the result
    """
    print(f"Loading data from {filepath}")
    if type == "url":
        df = load_csv_from_url(filepath)
    elif type == "path":
        df = load_csv_from_path(filepath)
    else:
        raise ValueError(f"Unknown type: {type}")
    
    print(f"Data loaded from {filepath}")

    return df
