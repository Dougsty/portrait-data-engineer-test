from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import sys

sys.path.append("../")

load_dotenv()


def postgres_connection(
    user: str = os.getenv("POSTGRES_USER"),
    password: str = os.getenv("POSTGRES_PASSWORD"),
    host: str = "localhost",
    port: str = "5432",
    database: str = "healthcare",
) -> create_engine:
    """
    Create a connection to the PostgreSQL database.
    Args:
        user (str): PostgreSQL username.
        password (str): PostgreSQL password.
        host (str): PostgreSQL host.
        port (str): PostgreSQL port.
        database (str): PostgreSQL database name.
    Returns:
        engine (create_engine): SQLAlchemy engine object.
    Raises:
        ValueError: If any required parameter is missing.
        Exception: If the connection to the database fails.
    """

    # Validate required parameters
    if not user or not password:
        raise ValueError("Database user and password must be provided.")

    try:
        # Create a connection string
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
        
        return engine
    except Exception as e:
        raise Exception(f"Failed to connect to the PostgreSQL database: {e}")
