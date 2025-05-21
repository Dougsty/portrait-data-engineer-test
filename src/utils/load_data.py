from connection import postgres_connection
import pandas as pd


def load_data(df: pd.DataFrame, table_name: str, schema: str) -> None:
    """
    Send data to PostgreSQL database.

    Args:
        df (pd.DataFrame): DataFrame to be sent.
        table_name (str): Name of the table in the database.
    """
    # Convert DataFrame to SQL and send it to PostgreSQL
    engine = postgres_connection()

    # Create schema if it doesn't exist
    with engine.connect() as connection:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # Check if the table already exists, for incremental or full load
    # If the table exists, append data; otherwise, replace it
    if engine.dialect.has_table(engine, table_name, schema=schema):
        first_run = "append"
    else:
        first_run = "replace"

    # Send DataFrame to the specified schema and table
    df.to_sql(table_name, con=engine, schema=schema, if_exists=first_run, index=False)

    print(f"Data sent to {schema}.{table_name} successfully.")