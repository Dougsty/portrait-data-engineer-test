import sys
import os

root_path = os.path.join(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(root_path)
from utils.load_data import load_data
import pandas as pd

dataset_path = os.path.join(os.path.dirname(root_path), "sample_datasets")


def load_all_raw_data():
    """Load all CSV files into PostgreSQL tables."""
    schema = "raw"
    for dataset in os.listdir(dataset_path):
        if dataset.endswith(".csv"):
            print(f"=== Loading {dataset}...")
            table_name = dataset.split(".")[0]  # Extract table name from filename
            data = pd.read_csv(os.path.join(dataset_path, dataset))
            load_data(df=data, table_name=table_name, schema=schema)
            print(f"--- Successfully loaded {table_name=} into {schema=}")

if __name__ == "__main__":
    load_all_raw_data()