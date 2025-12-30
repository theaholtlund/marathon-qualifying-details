# Import required libraries
import re
import argparse
import pandas as pd
from typing import Optional

# Import shared configuration and functions from other scripts
from config import logger, get_db_connection, RUNNER_AGE, RUNNER_GENDER, MARATHON_LOCATION, PERSONAL_BEST
from database import create_tables, insert_racedata, insert_qualifying_times
from scrape import scrape_london, scrape_boston, scrape_tokyo, scrape_berlin, scrape_chicago, scrape_new_york


def _format_time(seconds: int, signed: bool = False) -> str:
    """Format a time in seconds as H:MM:SS, optionally showing sign for positive/negative values."""
    sign = ""
    if signed:
        sign = "-" if seconds < 0 else "+"
        seconds = abs(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{sign}{hours}:{minutes:02d}:{secs:02d}"


def age_in_group(age: int, group: str) -> bool:
    age_group = group.lower().replace("–", "-").replace(" ", "")

    if age_group.endswith("+"):
        return age >= int(age_group[:-1])

    # Open-ended format, like "80andover"
    if "andover" in age_group:
        nums = re.findall(r"\d+", age_group)
        return bool(nums) and age >= int(nums[0])

    # Closed range, like "18-39"
    nums = list(map(int, re.findall(r"\d+", age_group)))
    if len(nums) == 2:
        return nums[0] <= age <= nums[1]

    # Exact age, e.g. "18"
    if len(nums) == 1:
        return age == nums[0]

    # Unrecognised or unparsable age-group format
    return False


def get_age_group(age: int, location: str) -> str:
    """Get age group label depending on marathon location."""
    location = (location or "").strip().lower()

    boston_style = {"boston", "chicago", "new york"}

    if location == "london":
        groups = [
            (18, 39), (40, 44), (45, 49), (50, 54),
            (55, 59), (60, 64), (65, 69), (70, 74),
            (75, 79), (80, 84), (85, 89), (90, 150)
        ]
        for lower, upper in groups:
            if lower <= age <= upper:
                return f"{lower}-{upper}" if upper < 90 else "90+"

    elif location in boston_style:
        groups = [
            (18, 34), (35, 39), (40, 44), (45, 49), (50, 54),
            (55, 59), (60, 64), (65, 69), (70, 74), (75, 79),
            (80, 150)
        ]
        for lower, upper in groups:
            if lower <= age <= upper:
                return "80+" if lower == 80 else f"{lower}-{upper}"

    elif location in {"tokyo", "berlin"}:
        lower = (age // 5) * 5
        upper = lower + 4
        if age >= 80:
            return "80+"
        return f"{lower}-{upper}"

    return "Unknown"


def display_runner_qualifying_times(cursor, age_group: str, gender: str, location: Optional[str] = None) -> None:
    """Display qualifying times for the given age group and gender."""
    sql = "SELECT AgeGroup, Women, Men, Location FROM dbo.QualifyingTimes WHERE AgeGroup = ?"
    params = [age_group]
    if location:
        sql += " AND Location = ?"
        params.append(location)
    cursor.execute(sql, *params)
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
    elif len(parts) == 1:
        h, m, s = 0, parts[0], 0
    else:
        return None
    return h * 3600 + m * 60 + s


def print_pb_margin(cursor, location: str, runner_age: int, gender: str, pb_text: str) -> None:
    """Print margin between runner personal best and qualifying standard (signed H:MM:SS)."""
    pb_seconds = parse_time_to_seconds(pb_text)
    if pb_seconds is None:
        logger.error(f"Could not parse personal best time '{pb_text}'. Expected H:MM:SS.")
        return

    seconds_column = "WomenSeconds" if gender.lower() == "women" else "MenSeconds"

    cursor.execute(f"""
        SELECT TOP 1 AgeGroup, {seconds_column}
        FROM dbo.QualifyingTimes
        WHERE Location = ?
          AND (
                AgeGroup LIKE '%and over%'
             OR AgeGroup NOT LIKE '%and over%'
          )
    """, location)

    rows = cursor.fetchall()

    matched = None
    for age_group, qualifying_seconds in rows:
        if qualifying_seconds is None:
            continue
        if age_in_group(runner_age, age_group):
            matched = (age_group, qualifying_seconds)
            break

    if not matched:
        logger.warning(f"No qualifying standard found for {location}, age {runner_age}.")
        return

    age_group, qualifying_seconds = matched
    delta = pb_seconds - qualifying_seconds

    print(
        f"{location} ({age_group}): "
        f"{_format_time(delta, signed=True)} "
        f"({pb_text} vs {_format_time(qualifying_seconds)})"
    )


def display_pb_margin_for_all_locations(cursor, runner_age: int, gender: str, pb_text: str) -> None:
    cursor.execute("SELECT DISTINCT Location FROM dbo.QualifyingTimes")
    for (loc,) in cursor.fetchall():
        print_pb_margin(cursor, loc, runner_age, gender, pb_text)


def run_pipeline(runner_age: int, runner_gender: str, override_location: Optional[str] = None, pb: Optional[str] = None) -> None:
    """Main end-to-end flow from scraping to database querying."""
    # Connect to database
    conn = get_db_connection()
    cursor = conn.cursor()

    logger.info("Creating tables if they do not exist, migrating schema")
    create_tables(cursor)
    conn.commit()

    logger.info("Scraping marathon data")
    datasets = [scrape_london(), scrape_boston(), scrape_tokyo(), scrape_berlin(), scrape_chicago(), scrape_new_york()]

    # Combine race data and qualifying times
    all_data = pd.concat([london_data, boston_data], ignore_index=True)
    all_times = pd.concat([london_times, boston_times], ignore_index=True)

    logger.info("Inserting data into database")
    insert_racedata(cursor, all_data)
    insert_qualifying_times(cursor, all_times)
    conn.commit()

    location = override_location or MARATHON_LOCATION
    age_group = get_age_group(runner_age, location)
    display_runner_qualifying_times(cursor, age_group, runner_gender)
    print(f"\nQualifying time for {location}, age group: {age_group}:")

    if pb:
        print("\nMargin between personal best time and qualifying time for all locations:")
        display_pb_margin_for_all_locations(cursor, runner_age, runner_gender, pb)

    cursor.close()
    conn.close()


def parse_args() -> argparse.Namespace:
    """Handle CLI arguments to override runner info."""
    parser = argparse.ArgumentParser(description="Marathon Qualifying Time Checker")
    parser.add_argument("--age", type=int, default=RUNNER_AGE, help="Runner's age")
    parser.add_argument("--gender", type=str, default=RUNNER_GENDER, help="Runner's gender (Men/Women)")
    parser.add_argument("--location", type=str, default=MARATHON_LOCATION, help="Override marathon location (London/Boston)")
    parser.add_argument("--pb", type=str, default=PERSONAL_BEST, help="Runner personal best as H:MM:SS to compute margin vs standard")
    return parser.parse_args()


def main() -> None:
    """Parse CLI arguments and run the marathon qualifying time pipeline."""
    args = parse_args()
    run_pipeline(args.age, args.gender, override_location=args.location, pb=args.pb)


if __name__ == "__main__":
    main()
