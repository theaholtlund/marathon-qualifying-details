# Import required libraries
import os
from dotenv import load_dotenv
from typing import Optional, Iterable, List


def _require_env_vars(names: Iterable[str]) -> None:
    missing: List[str] = [name for name in names if not os.getenv(name)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


def wake_database(cursor):
    """Issue a lightweight query to wake the database if it is paused."""
    try:
        cursor.execute("SELECT 1")
        cursor.fetchone()
    except Exception as e:
        raise RuntimeError(f"Failed to wake the database: {e}")


def get_db_connection() -> pyodbc.Connection:
    """Create and return pyodbc connection, raise error if required variables are missing."""

    driver = os.getenv("SQL_DRIVER")
    server = os.getenv("SQL_SERVER")
    port = os.getenv("SQL_PORT")
    database = os.getenv("SQL_DATABASE")
    uid = os.getenv("SQL_ADMIN_USER")
    pwd = os.getenv("SQL_ADMIN_PASSWORD")
    encrypt = os.getenv("SQL_ENCRYPT")
    trust_cert = os.getenv("SQL_TRUST_SERVER_CERTIFICATE")
    timeout = os.getenv("SQL_CONNECTION_TIMEOUT")

    if not all([driver, server, port, database, uid, pwd]):
        raise ValueError("Missing one or more required SQL environment variables")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"Server=tcp:{server},{port};"
        f"Database={database};"
        f"UID={uid};PWD={pwd};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust_cert};"
        f"Connection Timeout={timeout};"
    )

    connection: pyodbc.Connection = pyodbc.connect(conn_str)
    wake_database(connection.cursor())
    return connection

# Load environment variables from environment file
load_dotenv()

# Set runner profile
RUNNER_AGE = os.getenv("RUNNER_AGE", 30)
RUNNER_GENDER = os.getenv("RUNNER_GENDER", "Women")
MARATHON_LOCATION = os.getenv("MARATHON_LOCATION", "Boston")
PERSONAL_BEST = os.getenv("PERSONAL_BEST", "3:15:00")
