# Import required libraries
from config import get_db_connection
from database import create_tables, insert_racedata, insert_qualifying_times, query_top_times
from scrape import scrape_london, scrape_boston
import pandas as pd

def main():
    # Connect to database
    conn = get_db_connection()
    cursor = conn.cursor()

    print("Creating tables if not exist...")
    create_tables(cursor)
    conn.commit()

    print("Scraping London marathon data...")
    london_data, london_times = scrape_london()

    print("Scraping Boston marathon data...")
    boston_data, boston_times = scrape_boston()

    # Combine race data and qualifying times
    all_data = pd.concat([london_data, boston_data], ignore_index=True)
    all_times = pd.concat([london_times, boston_times], ignore_index=True)

    print("Inserting race data into database...")
    insert_racedata(cursor, all_data)

    print("Inserting qualifying times into database...")
    insert_qualifying_times(cursor, all_times)

    conn.commit()

    print("\nSample data from RaceData table:")
    cursor.execute("SELECT TOP 5 * FROM dbo.RaceData;")
    for row in cursor.fetchall():
        print(row)

    print("\nSample data from QualifyingTimes table:")
    cursor.execute("SELECT TOP 5 * FROM dbo.QualifyingTimes;")
    for row in cursor.fetchall():
        print(row)

    # Query top 5 qualifying times for Boston only
    print("\nTop 5 qualifying times for Boston:")
    rows = query_top_times(cursor, location="Boston", limit=5)
    for row in rows:
        print(row)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
