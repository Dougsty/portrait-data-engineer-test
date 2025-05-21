import sys
import os

root_path = os.path.join(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(root_path)
from utils.connection import postgres_connection
from utils.load_data import load_data
import pandas as pd


def transform_appointment(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df["appointment_date"] = pd.to_datetime(df["appointment_date"], format="%Y-%m-%d")
    except Exception as e:
        raise ValueError(f"Error transforming appointment data: {e}")
    return df


def transform_patients(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df["registration_date"] = pd.to_datetime(df["registration_date"], format="%Y-%m-%d")
        df["age"] = df["age"].astype("Int64")
    except Exception as e:
        raise ValueError(f"Error transforming patients data: {e}")
    return df


def transform_prescriptions(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df["prescription_date"] = pd.to_datetime(df["prescription_date"], format="%Y-%m-%d")
    except Exception as e:
        raise ValueError(f"Error transforming prescriptions data: {e}")
    return df


# Just for pattern purpose
def transform_providers(df: pd.DataFrame) -> pd.DataFrame:
    return df


data_to_transform = {
    "patients": transform_patients,
    "appointments": transform_appointment,
    "providers": transform_providers,
    "prescriptions": transform_prescriptions,
}


def transform_all_data():
    """Transform data from raw to trusted layer"""
    for table_name, transform_func in data_to_transform.items():
        try:
            query = f"SELECT * FROM healthcare.raw.{table_name}"
            df = pd.read_sql(query, con=postgres_connection())
            transformed_df = transform_func(df)
            load_data(df=transformed_df, table_name=table_name, schema="trusted")
        except Exception as e:
            print(f"Error processing table {table_name}: {e}")


if __name__ == "__main__":
    transform_all_data()
