import logging
from pathlib import Path

import pandas as pd


def config_logging():
    fmt = "%(asctime)s\t%(module)s.%(funcName)s (line %(lineno)d)\t%(levelname)s : %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt)


def create_dir(dir: Path):
    """Check if a directory already exists. Otherwise, create it."""
    logging.info(f'Creating directory {dir}')
    if not dir.exists():
        dir.mkdir(parents=True, exist_ok=True)
    else:
        logging.info(f'Directory {dir} already exists.')


def create_dataframe(titles, zone, prices, areas, condition, created, descriptions,
                     uris) -> pd.DataFrame:
    """Create a Pandas DataFrame containing the properties extracted data."""
    col_names = ['Title', 'Zone', 'Price (€)', 'Size (m²)', 'Status', 'Description', 'Date', 'URI']
    df = pd.DataFrame({'Title': titles,
                       'Price': prices,
                       'Size (m²)': areas,
                       'Zone': zone,
                       'Status': condition,
                       'Date': created,
                       'Description': descriptions,
                       'URI': uris})[col_names]
    return df


def save_df_to_csv(df: pd.DataFrame, filepath: Path):
    """Save a Pandas DataFrame to a CSV file."""
    logging.info(f'Saving Pandas DataFrame to {filepath}')
    df.to_csv(filepath)
