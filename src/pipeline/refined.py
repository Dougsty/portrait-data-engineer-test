import sys
import os

root_path = os.path.join(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(root_path)
from utils.connection import postgres_connection
from utils.load_data import load_data
import pandas as pd


def refinement_appointment(df: pd.DataFrame) -> pd.DataFrame:
    df["day_of_week"] = df["appointment_date"].dt.day_name()
    df["days_since_last_appointment"] = (
        pd.to_datetime("now") - df["appointment_date"]
    ).dt.days
    return df


def refinement_patients(df: pd.DataFrame) -> pd.DataFrame:
    # Aggregations

    df["age_group"] = pd.cut(
        df["age"],
        bins=[0, 18, 30, 50, 70, 100],
        labels=["0-18", "19-30", "31-50", "51-70", "71+"],
    )

    # Create a new column for the number of months since registration
    df["months_since_registration"] = (
        pd.to_datetime("now") - df["registration_date"]
    ).dt.days // 30
    df["months_since_registration"] = df["months_since_registration"].astype("Int64")

    df["patient_type"] = pd.cut(
        df["months_since_registration"],
        bins=[0, 6, 24, 100],
        labels=["New", "Regular", "Old"],
    )

    return df


def refinement_prescriptions(df: pd.DataFrame) -> pd.DataFrame:
    medication_category_mapping = {
        "Ibuprofen": "Pain Relief",
        "Metformin": "Diabetes",
        "Lisinopril": "Heart",
        "Atorvastatin": "Heart",
        "Amoxicillin": "Antibiotic",
        "Aspirin": "Pain Relief",
    }

    df_frequency = (
        df.groupby(["patient_id"])
        .agg(
            prescription_frequency=("prescription_id", "count"),
        )
        .reset_index()
    )

    df = df.merge(df_frequency, on="patient_id", how="left")

    df["prescription_frequency"] = pd.cut(
        df["prescription_frequency"], bins=[0, 1, 100], labels=["First-time", "Repeat"]
    )

    df["medication_category"] = df["medication_name"].map(medication_category_mapping)
    return df


# Just for patern propose
def refinement_providers(df: pd.DataFrame) -> pd.DataFrame:
    return df


data_to_refinement = {
    "patients": refinement_patients,
    "appointments": refinement_appointment,
    "providers": refinement_providers,
    "prescriptions": refinement_prescriptions,
}


def refine_all_data():
    """Transform data from trusted to refined layer"""
    for table_name, transform_func in data_to_refinement.items():
        query = f"SELECT * FROM healthcare.trusted.{table_name}"
        df = pd.read_sql(query, con=postgres_connection())
        transformed_df = transform_func(df)
        load_data(df=transformed_df, table_name=table_name, schema="refined")


if __name__ == "__main__":
    refine_all_data()
