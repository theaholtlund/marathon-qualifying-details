# Import required libraries
import argparse
import pandas as pd
from typing import Optional

# Import shared configuration and functions from other scripts
from config import logger, get_db_connection, RUNNER_AGE, RUNNER_GENDER, MARATHON_LOCATION, PERSONAL_BEST
from database import create_tables, insert_racedata, insert_qualifying_times, query_top_times
from scrape import scrape_london, scrape_boston


def get_age_group(age, location):
    """Get age group label depending on marathon location."""
    location = (location or "").strip().lower()
    if location == "london":
        groups = [
            (18, 39), (40, 44), (45, 49), (50, 54),
            (55, 59), (60, 64), (65, 69), (70, 74),
            (75, 79), (80, 84), (85, 89), (90, 150)
        ]
        for lower, upper in groups:
            if lower <= age <= upper:
                return f"{lower}-{upper}" if upper < 90 else "90+"
    elif location == "boston":
        groups = [
            (18, 34), (35, 39), (40, 44), (45, 49), (50, 54),
            (55, 59), (60, 64), (65, 69), (70, 74), (75, 79),
            (80, 150)
        ]
        for lower, upper in groups:
            if lower <= age <= upper:
                return "80+" if lower == 80 else f"{lower}-{upper}"
    return "Unknown"


def display_runner_qualifying_times(cursor, age_group, gender):
    """Print qualifying times for the given age group and gender."""
    cursor.execute("""
        SELECT AgeGroup, Women, Men, Location
        FROM dbo.QualifyingTimes
        WHERE AgeGroup = ?
    """, age_group)

    results = cursor.fetchall()

    if not results:
        logger.warning(f"No qualifying times found for age group: {age_group}")
        return
    
    print(f"\nQualifying times for age group: {age_group} and gender: {gender}")
    for row in results:
        location = row[3]
        time_text = row[1] if gender.lower() == "women" else row[2]
        print(f"{location}: {time_text}")


def parse_time_to_seconds(text: Optional[str]) -> Optional[int]:
    """Convert 'H:MM:SS' or 'M:SS' or 'sub 3:00:00' to seconds."""
    if not text:
        return None
    t = text.strip().lower().replace("sub", "").strip()
    t = t.replace("hrs", ":").replace("hr", ":").replace("min", ":").replace("sec", "")
    parts = [p for p in t.split(":") if p]
    try:
        parts = [int(p) for p in parts]
    except ValueError:
        return None
    if len(parts) == 3:
        h, m, s = parts
    elif len(parts) == 2:
        h, m, s = 0, parts[0], parts[1]
    else:
        return None
    return h * 3600 + m * 60 + s


def print_pb_margin(cursor, location: str, age_group: str, gender: str, pb_text: str) -> None:
    """Print margin between runner personal best and qualifying standard, in H:MM:SS format."""
    pb_secs = parse_time_to_seconds(pb_text)
    if pb_secs is None:
        print(f"! Could not parse personal best time '{pb_text}'. Expected H:MM:SS.")
        return

    # Choose numeric column based on gender
    column_secs = "WomenSeconds" if gender.lower() == "women" else "MenSeconds"
    cursor.execute(f"""
        SELECT {column_secs}
        FROM dbo.QualifyingTimes
        WHERE Location = ? AND AgeGroup = ?
    """, (location, age_group))
    row = cursor.fetchone()
    if not row or row[0] is None:
        logger.warning(f"No numeric qualifying standard available for {location} and age group {age_group}.")
        return

    q_secs = row[0]
    delta = pb_secs - q_secs  # Negative means faster than standard
    sign = "-" if delta < 0 else "+"
    print(f"* Personal best margin vs {location} standard: {sign}{abs(delta)} seconds ({pb_text} vs {age_group})")


def run_pipeline(runner_age, runner_gender, override_location=None, pb=None):
    """Main end-to-end flow from scraping to database querying."""
    # Connect to database
    conn = get_db_connection()
    cursor = conn.cursor()

    logger.info("Creating tables if they do not exist, migrating schema")
    create_tables(cursor)
    conn.commit()

    print("* Scraping marathon data")
    try:
        london_data, london_times = scrape_london()
        boston_data, boston_times = scrape_boston()
    except Exception as e:
        print(f"! Error during scraping: {e}")
        return

    # Combine race data and qualifying times
    all_data = pd.concat([london_data, boston_data], ignore_index=True)
    all_times = pd.concat([london_times, boston_times], ignore_index=True)

    logger.info("Inserting data into database")
    insert_racedata(cursor, all_data)
    insert_qualifying_times(cursor, all_times)
    conn.commit()

    print("\n* Sample data from race data table:")
    cursor.execute("SELECT TOP 5 * FROM dbo.RaceData;")
    for row in cursor.fetchall():
        print(row)

    print("\n* Sample data from qualifying times table:")
    cursor.execute("SELECT TOP 5 * FROM dbo.QualifyingTimes;")
    for row in cursor.fetchall():
        print(row)

    location = override_location or MARATHON_LOCATION
    age_group = get_age_group(runner_age, location)
    display_runner_qualifying_times(cursor, age_group, runner_gender)

    print(f"\n* Qualifying time for {location}, age group: {age_group}:")
    try:
        for row in query_top_times(
            cursor,
            location=location,
            age_group=age_group,
            gender=runner_gender,
            limit=1
        ):
            print(row)
    except ValueError as e:
        print(f"! Error fetching top times: {e}")

    if pb:
        print_pb_margin(cursor, location, age_group, runner_gender, pb)

    cursor.close()
    conn.close()


def parse_args():
    """Handle CLI arguments to override runner info."""
    parser = argparse.ArgumentParser(description="Marathon Qualifying Time Checker")
    parser.add_argument("--age", type=int, default=RUNNER_AGE, help="Runner's age")
    parser.add_argument("--gender", type=str, default=RUNNER_GENDER, help="Runner's gender (Men/Women)")
    parser.add_argument("--location", type=str, default=MARATHON_LOCATION, help="Override marathon location (London/Boston)")
    parser.add_argument("--pb", type=str, default=PERSONAL_BEST, help="Runner personal best as H:MM:SS to compute margin vs standard")
    return parser.parse_args()


def main():
    args = parse_args()
    run_pipeline(args.age, args.gender, override_location=args.location, pb=args.pb)


if __name__ == "__main__":
    main()
