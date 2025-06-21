# Import required libraries
import os
from dotenv import load_dotenv
import pyodbc

# Load environment variables from env file
load_dotenv()

def get_db_connection():
    """
    Create and return a pyodbc connection using the env connection string.
    """
    conn_str = os.getenv("SQL_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("SQL_CONNECTION_STRING not set in environment")
    conn = pyodbc.connect(conn_str)
    return conn
