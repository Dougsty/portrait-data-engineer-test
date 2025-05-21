from .connection import postgres_connection
import pandas as pd
from sqlalchemy import text
from sqlalchemy import inspect


def load_data(df: pd.DataFrame, table_name: str, schema: str) -> None:
    """
    Send data to PostgreSQL database.

    Args:
        df (pd.DataFrame): DataFrame to be sent.
        table_name (str): Name of the table in the database.
        schema (str): Schema where the table will be created.
    """
    # Convert DataFrame to SQL and send it to PostgreSQL
    engine = postgres_connection()

    # Create schema if it doesn't exist

    with engine.connect() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        connection.commit()

    # Check for incremental or full load
    inspector = inspect(engine)
    if inspector.has_table(table_name, schema=schema):
        first_run = "append"
    else:
        first_run = "replace"

    df['date_insertion'] = pd.to_datetime("now")
    # Send DataFrame to the specified schema and table
    df.to_sql(table_name, con=engine, schema=schema, if_exists=first_run, index=False)

    print(
        f"=== Data sent to {schema}.{table_name} with {first_run} operation successfully!"
    )
