# Import required libraries
import re
import pandas as pd
from typing import Optional, Iterable, Tuple

# Import shared configuration and functions from other scripts
from config import logger


def time_to_seconds(time_str: Optional[str]) -> Optional[int]:
    """Convert a time string to seconds. Supports 'sub H:MM', 'sub HH:MM', 'Hhrs Mmin Ssec' or 'HHhrs MMmin SSsec'."""
    if not time_str:
        return None
    time_str = time_str.lower().strip()

    # London format is for example "sub 3:38"
    match = re.match(r"sub (\d+):(\d+)(?::(\d+))?", time_str)
    if match:
        h = int(match.group(1))
        m = int(match.group(2))
        s = int(match.group(3)) if match.group(3) else 0
        return h*3600 + m*60 + s

    # Boston format is for example "3hrs 25min 00sec"
    match = re.match(r"(?:(\d+)hrs?)?\s*(?:(\d+)min)?\s*(?:(\d+)sec)?", time_str)
    if match:
        h, m, s = match.groups()
        h = int(h) if h else 0
        m = int(m) if m else 0
        s = int(s) if s else 0
        return h*3600 + m*60 + s

    return None


def _add_column_if_missing(cursor, table, column, definition):
    """Add a column to a table if it is not already present."""
    cursor.execute("""
        SELECT 1
        FROM sys.columns
        WHERE object_id = OBJECT_ID(?) AND name = ?
    """, (f"dbo.{table}", column))
    if cursor.fetchone() is None:
        logger.info(f"Adding column {column} to {table}.")
        cursor.execute(f"ALTER TABLE dbo.{table} ADD {column} {definition}")


def create_tables(cursor):
    """Create tables if they do not already exist, and perform lightweight schema migrations."""
    logger.info("Ensuring race data and qualifying times tables exist")

    # Create the race data table
    cursor.execute("""
    IF OBJECT_ID('dbo.RaceData', 'U') IS NULL
    CREATE TABLE dbo.RaceData (
        RaceYear INT,
        Location NVARCHAR(50),
        QualifyingText NVARCHAR(MAX),
        LinkText NVARCHAR(255),
        LinkURL NVARCHAR(255),
        ScrapeDate DATETIME,
        PageHash NVARCHAR(64) NULL
    );
    """)

    # Add unique constraint for table if it is missing
    cursor.execute("""
    IF NOT EXISTS (
        SELECT * FROM sys.indexes 
        WHERE name = 'UQ_RaceData' AND object_id = OBJECT_ID('dbo.RaceData')
    )
    BEGIN
        ALTER TABLE dbo.RaceData
        ADD CONSTRAINT UQ_RaceData UNIQUE (RaceYear, Location);
    END
    """)

    # Create the qualifying times table
    cursor.execute("""
    IF OBJECT_ID('dbo.QualifyingTimes', 'U') IS NULL
    CREATE TABLE dbo.QualifyingTimes (
        AgeGroup NVARCHAR(20),
        Women NVARCHAR(20),
        Men NVARCHAR(20),
        Location NVARCHAR(50)
    );
    """)

    # Add unique constraint for table
    cursor.execute("""
    IF NOT EXISTS (
        SELECT * FROM sys.indexes 
        WHERE name = 'UQ_QualTimes' AND object_id = OBJECT_ID('dbo.QualifyingTimes')
    )
    BEGIN
        ALTER TABLE dbo.QualifyingTimes
        ADD CONSTRAINT UQ_QualTimes UNIQUE (AgeGroup, Location);
    END
    """)


def insert_racedata(cursor, df: pd.DataFrame) -> None:
    """Insert or update race metadata into the race data table."""
    for _, row in df.iterrows():
        cursor.execute(
            "SELECT COUNT(*) FROM dbo.RaceData WHERE RaceYear=? AND Location=?",
            row.RaceYear, row.Location
        )
        exists = cursor.fetchone()[0]
        if exists:
            cursor.execute(
                "UPDATE dbo.RaceData SET QualifyingText=?, LinkText=?, LinkURL=?, ScrapeDate=?, PageHash=? "
                "WHERE RaceYear=? AND Location=?",
                row.QualifyingText, row.LinkText, row.LinkURL, row.ScrapeDate, row.get("PageHash", None),
                row.RaceYear, row.Location
            )
        else:
            cursor.execute(
                "INSERT INTO dbo.RaceData (RaceYear,Location,QualifyingText,LinkText,LinkURL,ScrapeDate,PageHash) "
                "VALUES (?,?,?,?,?,?,?)",
                row.RaceYear, row.Location, row.QualifyingText, row.LinkText, row.LinkURL, row.ScrapeDate, row.get("PageHash", None)
            )


def insert_qualifying_times(cursor, df, verbose=True):
    """Insert or update qualifying times into the qualifying times table, including numeric seconds."""
    for _, row in df.iterrows():
        age_group = row["Age Group"]
        location = row["Location"]

        women_sec = time_to_seconds(row.get("Women"))
        men_sec = time_to_seconds(row.get("Men"))

        cursor.execute(
            "SELECT COUNT(*) FROM dbo.QualifyingTimes WHERE AgeGroup=? AND Location=?",
            age_group, location
        )
        exists = cursor.fetchone()[0]
        if exists:
            cursor.execute(
                "UPDATE dbo.QualifyingTimes SET Women=?, Men=?, WomenSeconds=?, MenSeconds=? WHERE AgeGroup=? AND Location=?",
                row["Women"], row["Men"], women_sec, men_sec, age_group, location
            )
        else:
            cursor.execute(
                "INSERT INTO dbo.QualifyingTimes (AgeGroup, Women, Men, Location, WomenSeconds, MenSeconds) VALUES (?, ?, ?, ?, ?, ?)",
                age_group, row["Women"], row["Men"], location, women_sec, men_sec
            )


def query_top_times(cursor, location: str, age_group: str, gender: str, limit: int = 5) -> Iterable[Tuple]:
    """Query qualifying time by location, age group and gender."""
    if not location or not age_group or not gender:
        raise ValueError("Location, age group and gender must be provided")

    limit_int = int(limit)

    is_women = gender.strip().lower() == "women"
    text_col = "Women" if is_women else "Men"

    sql = f"""
        SELECT TOP {limit_int} AgeGroup, {text_col}, Location
        FROM dbo.QualifyingTimes
        WHERE Location = ? AND AgeGroup = ?
    """
    cursor.execute(sql, location, age_group)
    return cursor.fetchall()
