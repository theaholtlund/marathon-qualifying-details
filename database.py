# Import required libraries
import pandas as pd
from typing import Optional, Tuple

# Import shared configuration and functions from other scripts
from config import logger


def normalise_time(text: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
    """Normalise marathon qualifying times to H:MM:SS and total seconds."""
    if not text:
        return None, None

    # Convert time phrases into colon-separated format for consistent parsing
    t = (
        str(text)
        .lower()
        .replace("sub", "")
        .replace("under", "")
        .replace("hrs", ":")
        .replace("hr", ":")
        .replace("min", ":")
        .replace("sec", "")
        .strip()
    )

    parts = [p for p in t.split(":") if p.strip().isdigit()]
    if not parts:
        return None, None

    # Keep only numeric components to avoid issues with stray text or symbols
    parts = [int(p) for p in parts]

    if len(parts) == 3:
        h, m, s = parts
    elif len(parts) == 2:
        h, m = parts
        s = 0
    elif len(parts) == 1:
        h, m, s = parts[0], 0, 0
    else:
        return None, None

    seconds = h * 3600 + m * 60 + s
    return f"{h}:{m:02d}:{s:02d}", seconds


def normalise_age_group(text: Optional[str]) -> Optional[str]:
    """Normalise age group text by standardising dashes and whitespace."""
    if not text:
        return text
    return (str(text).replace("–", "-").replace("—", "-").replace("  ", " ").strip())


def create_tables(cursor) -> None:
    """Create tables if they do not already exist and perform lightweight schema migrations."""
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
        Location NVARCHAR(50),
        WomenSeconds INT NULL,
        MenSeconds INT NULL
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
                row.QualifyingText, row.LinkText, row.LinkURL, row.ScrapeDate, row.get("PageHash"),
                row.RaceYear, row.Location
            )
        else:
            cursor.execute(
                "INSERT INTO dbo.RaceData (RaceYear,Location,QualifyingText,LinkText,LinkURL,ScrapeDate,PageHash) "
                "VALUES (?,?,?,?,?,?,?)",
                row.RaceYear, row.Location, row.QualifyingText, row.LinkText, row.LinkURL, row.ScrapeDate, row.get("PageHash", None)
            )


def insert_qualifying_times(cursor, df: pd.DataFrame) -> None:
    """Insert or update qualifying times into the qualifying times table, including qualifying time in seconds."""
    for _, row in df.iterrows():
        age_group = normalise_age_group(row.get("Age Group"))
        location = row.get("Location")

        women_text, women_sec = normalise_time(row.get("Women"))
        men_text, men_sec = normalise_time(row.get("Men"))

        cursor.execute(
            "SELECT COUNT(*) FROM dbo.QualifyingTimes WHERE AgeGroup=? AND Location=?",
            age_group, location
        )

        exists = cursor.fetchone()[0]

        if exists:
            cursor.execute("""
                UPDATE dbo.QualifyingTimes
                SET Women=?, Men=?, WomenSeconds=?, MenSeconds=?
                WHERE AgeGroup=? AND Location=?

            """, women_text, men_text, women_sec, men_sec, age_group, location)
        else:
            cursor.execute("""
                INSERT INTO dbo.QualifyingTimes
                (AgeGroup, Women, Men, Location, WomenSeconds, MenSeconds)
                VALUES (?, ?, ?, ?, ?, ?)
            """, age_group, women_text, men_text, location, women_sec, men_sec)
