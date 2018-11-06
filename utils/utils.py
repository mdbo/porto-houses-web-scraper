import logging
from pathlib import Path
from typing import List

import pandas as pd


def config_logging():
    fmt = "%(asctime)s\t%(module)s.%(funcName)s (line %(lineno)d)\t%(levelname)s : %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt)


def create_dir(dir: Path):
    """Check if a directory already exists. Otherwise, create it."""
    logging.info(f'Creating directory {dir}')
    if dir.exists():
        logging.info(f'Directory {dir} already exists.')
    else:
        dir.mkdir(parents=True, exist_ok=True)


def create_dataframe(titles: List[str], prices: List[int], sizes: List[str], zones: List[str],
                     conditions: List[str], dates: List[str], descriptions: List[str],
                     links: List[str]) -> pd.DataFrame:
    """Create a Pandas DataFrame containing the properties extracted data.
    NB: Price is in euros (€); Size is in square metre (m2)"""
    col_names = ['title', 'price', 'size', 'zone', 'status', 'date', 'description', 'uri']
    df = pd.DataFrame({'title': titles,
                       'price': prices,
                       'size': sizes,
                       'zone': zones,
                       'status': conditions,
                       'date': dates,
                       'description': descriptions,
                       'uri': links})[col_names]
    return df


def save_df_to_csv(df: pd.DataFrame, filepath: Path):
    """Save a Pandas DataFrame to a CSV file."""
    logging.info(f'Saving Pandas DataFrame to {filepath}')
    df.to_csv(filepath)
