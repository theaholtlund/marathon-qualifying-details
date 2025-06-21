# Import required libraries
from config import get_db_connection
from database import create_tables, insert_metadata, insert_qualifying_times
from scrape import scrape_london, scrape_boston
import pandas as pd

def main():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
