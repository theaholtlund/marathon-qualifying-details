# Import required libraries
import argparse
import pandas as pd
from config import get_db_connection, RUNNER_AGE, RUNNER_GENDER
from database import (
    create_tables,
    insert_racedata,
    insert_qualifying_times,
    query_top_times
)
from scrape import scrape_london, scrape_boston


def get_age_group(age):
    """
    Map runner's age to a marathon age group string format.
    Example: age 27 → '18-34'
    """
    age_groups = [
        (18, 34), (35, 39), (40, 44), (45, 49),
        (50, 54), (55, 59), (60, 64), (65, 69),
        (70, 74), (75, 79), (80, 84), (85, 89), (90, 120)
    ]
    for lower, upper in age_groups:
        if lower <= age <= upper:
            return f"{lower}-{upper}"
    return "Unknown"


def display_runner_qualifying_times(cursor, age_group, gender):
    """
    Display the qualifying time for the configured runner.
    """
    cursor.execute("""
        SELECT AgeGroup, Women, Men, Location
        FROM dbo.QualifyingTimes
        WHERE AgeGroup = ?
    """, age_group)

    results = cursor.fetchall()

    if not results:
        print(f"No qualifying times found for age group: {age_group}")
        return
    
    print(f"\nQualifying times for Age Group: {age_group} and Gender: {gender}")
    for row in results:
        location = row[3]
        time = row[1] if gender.lower() == "women" else row[2]
        print(f"{location}: {time}")

def main():
    # Connect to database
    conn = get_db_connection()
    cursor = conn.cursor()

    print("[*] Creating tables if they do not exist...")
    create_tables(cursor)
    conn.commit()

    print("Scraping webpages for marathon data...")
    london_data, london_times = scrape_london()
    boston_data, boston_times = scrape_boston()

    # Combine race data and qualifying times
    all_data = pd.concat([london_data, boston_data], ignore_index=True)
    all_times = pd.concat([london_times, boston_times], ignore_index=True)

    print("[*] Inserting data into database...")
    insert_racedata(cursor, all_data)
    insert_qualifying_times(cursor, all_times)
    conn.commit()

    print("\n[*] Sample data from RaceData table:")
    cursor.execute("SELECT TOP 5 * FROM dbo.RaceData;")
    for row in cursor.fetchall():
        print(row)

    print("\n[*] Sample data from QualifyingTimes table:")
    cursor.execute("SELECT TOP 5 * FROM dbo.QualifyingTimes;")
    for row in cursor.fetchall():
        print(row)

    # Show times for the configured runner
    age_group = get_age_group(RUNNER_AGE)
    display_runner_qualifying_times(cursor, age_group, RUNNER_GENDER)

    # Query top 5 Boston times
    print("\nTop 5 qualifying times for Boston:")
    for row in query_top_times(cursor, location="Boston", limit=5):
        print(row)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
