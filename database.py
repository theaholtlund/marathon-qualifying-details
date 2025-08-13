# Import required libraries
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_tables(cursor):
    """
    Create the race data and qualifying times tables if they do not already exist.
    """
    logger.info("Ensuring race data and qualifying times tables exist...")

    # Create RaceData table
    cursor.execute("""
    IF OBJECT_ID('dbo.RaceData', 'U') IS NULL
    CREATE TABLE dbo.RaceData (
        RaceYear INT,
        Location NVARCHAR(50),
        QualifyingText NVARCHAR(MAX),
        LinkText NVARCHAR(255),
        LinkURL NVARCHAR(255),
        ScrapeDate DATETIME
    );
    """)

    # Add unique constraint for table
    cursor.execute("""
    IF NOT EXISTS (
        SELECT * FROM sys.indexes 
        WHERE name = 'UQ_RaceData' AND object_id = OBJECT_ID('dbo.RaceData')
    )
    ALTER TABLE dbo.RaceData
    ADD CONSTRAINT UQ_RaceData UNIQUE (RaceYear, Location);
    """)

    # Create QualifyingTimes table
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
    ALTER TABLE dbo.QualifyingTimes
    ADD CONSTRAINT UQ_QualTimes UNIQUE (AgeGroup, Location);
    """)

def insert_racedata(cursor, df, verbose=True):
    """
    Insert or update race metadata into the race data table.
    """
    for _, row in df.iterrows():
        cursor.execute(
            "SELECT COUNT(*) FROM dbo.RaceData WHERE RaceYear=? AND Location=?",
            row.RaceYear, row.Location
        )
        exists = cursor.fetchone()[0]
        if exists:
            cursor.execute(
                "UPDATE dbo.RaceData SET QualifyingText=?,LinkText=?,LinkURL=?,ScrapeDate=? "
                "WHERE RaceYear=? AND Location=?",
                row.QualifyingText, row.LinkText, row.LinkURL, row.ScrapeDate, row.RaceYear, row.Location
            )
        else:
            cursor.execute(
                "INSERT INTO dbo.RaceData VALUES (?,?,?,?,?,?)",
                row.RaceYear, row.Location, row.QualifyingText, row.LinkText, row.LinkURL, row.ScrapeDate
            )

def insert_qualifying_times(cursor, df, verbose=True):
    """
    Insert or update qualifying times into the qualifying times table.
    """
    for _, row in df.iterrows():
        age_group = row["Age Group"]
        location = row["Location"]

        cursor.execute(
            "SELECT COUNT(*) FROM dbo.QualifyingTimes WHERE AgeGroup=? AND Location=?",
            age_group, location
        )
        exists = cursor.fetchone()[0]
        if exists:
            cursor.execute(
                "UPDATE dbo.QualifyingTimes SET Women=?, Men=? WHERE AgeGroup=? AND Location=?",
                row["Women"], row["Men"], age_group, location
            )
        else:
            cursor.execute(
                "INSERT INTO dbo.QualifyingTimes VALUES (?, ?, ?, ?)",
                age_group, row["Women"], row["Men"], location
            )

def query_top_times(cursor, location=None, age_group=None, gender=None, limit=5):
    """
    Query qualifying time by location, age group and gender.
    """
    if not location or not age_group or not gender:
        raise ValueError("location, age_group, and gender must be provided")

    column = "Women" if gender.lower() == "women" else "Men"
    cursor.execute(f"""
        SELECT TOP (?) AgeGroup, {column}, Location
        FROM dbo.QualifyingTimes
        WHERE Location = ? AND AgeGroup = ?
        ORDER BY AgeGroup
    """, (limit, location, age_group))

    return cursor.fetchall()
