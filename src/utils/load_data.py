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
    try:
        # Convert DataFrame to SQL and send it to PostgreSQL
        engine = postgres_connection()

        # Create schema if it doesn't exist
        with engine.connect() as connection:
            try:
                connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                connection.commit()
            except Exception as e:
                raise RuntimeError(f"Error creating schema '{schema}': {e}")

        # Check for incremental or full load
        inspector = inspect(engine)
        if inspector.has_table(table_name, schema=schema):
            first_run = "append"
        else:
            first_run = "replace"

        df["date_insertion"] = pd.to_datetime("now")
        # Send DataFrame to the specified schema and table
        try:
            df.to_sql(table_name, con=engine, schema=schema, if_exists=first_run, index=False)
        except Exception as e:
            raise RuntimeError(f"Error loading data into table '{schema}.{table_name}': {e}")

        print(
            f"=== Data sent to {schema}.{table_name} with {first_run} operation successfully!"
        )
    except Exception as e:
        print(f"An error occurred: {e}")
