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
    Convert an integer age into a standard age group category.
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
    Print qualifying times for the given age group and gender.
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


def run_pipeline(runner_age, runner_gender):
    """
    Main end-to-end flow from scraping to DB querying.
    """
    # Connect to database
    conn = get_db_connection()
    cursor = conn.cursor()

    print("[*] Creating tables if they do not exist...")
    create_tables(cursor)
    conn.commit()

    print("[*] Scraping marathon data...")
    try:
        london_data, london_times = scrape_london()
        boston_data, boston_times = scrape_boston()
    except Exception as e:
        print(f"[!] Error during scraping: {e}")
        return

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

    age_group = get_age_group(runner_age)
    display_runner_qualifying_times(cursor, age_group, runner_gender)

    print("\n[*] Top 5 Boston qualifying times by AgeGroup:")
    for row in query_top_times(cursor, location="Boston", limit=5, gender=runner_gender):
        print(row)

    cursor.close()
    conn.close()


def parse_args():
    """
    Handle CLI arguments to override runner info.
    """
    parser = argparse.ArgumentParser(description="Marathon Qualifying Time Checker")
    parser.add_argument("--age", type=int, default=RUNNER_AGE, help="Runner's age")
    parser.add_argument("--gender", type=str, default=RUNNER_GENDER, help="Runner's gender (Men/Women)")
    return parser.parse_args()


def main():
    args = parse_args()
    run_pipeline(args.age, args.gender)


if __name__ == "__main__":
    main()
