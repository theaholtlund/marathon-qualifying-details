def create_tables(cursor):
    """
    Create tables RaceData and QualifyingTimes if they do not exist.
    RaceData stores race year, location, qualifying text and reference links.
    QualifyingTimes stores qualifying times by age group and location.
    """
    # Create RaceData table
    cursor.execute("""
    IF OBJECT_ID('dbo.RaceData', 'U') IS NULL
    CREATE TABLE dbo.RaceData (
        RaceYear INT,
        Location NVARCHAR(50),
        QualifyingText NVARCHAR(MAX),
        LinkText NVARCHAR(255),
        LinkURL NVARCHAR(255)
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


def insert_racedata(cursor, df_racedata):
    """
    Insert or update race data into RaceData table.
    Ensures no duplicate entries for RaceYear and Location.
    """
    for _, row in df_racedata.iterrows():
        # Check if the record already exists
        cursor.execute("""
            SELECT COUNT(*) FROM dbo.RaceData WHERE RaceYear = ? AND Location = ?
        """, row.RaceYear, row.Location)

        exists = cursor.fetchone()[0] > 0

        if exists:
            # Update existing entry
            cursor.execute("""
                UPDATE dbo.RaceData
                SET QualifyingText = ?, LinkText = ?, LinkURL = ?
                WHERE RaceYear = ? AND Location = ?
            """, row.QualifyingText, row.LinkText, row.LinkURL, row.RaceYear, row.Location)
        else:
            # Insert new entry
            cursor.execute("""
                INSERT INTO dbo.RaceData (RaceYear, Location, QualifyingText, LinkText, LinkURL)
                VALUES (?, ?, ?, ?, ?)
            """, row.RaceYear, row.Location, row.QualifyingText, row.LinkText, row.LinkURL)


def insert_qualifying_times(cursor, df_times):
    """
    Insert or update qualifying times into QualifyingTimes table.
    Ensures no duplicate entries for AgeGroup and Location.
    """
    for _, row in df_times.iterrows():
        cursor.execute("""
            SELECT COUNT(*) FROM dbo.QualifyingTimes
            WHERE AgeGroup = ? AND Location = ?
        """, row["Age Group"], row["Location"])

        exists = cursor.fetchone()[0] > 0

        if exists:
            # Update existing
            cursor.execute("""
                UPDATE dbo.QualifyingTimes
                SET Women = ?, Men = ?
                WHERE AgeGroup = ? AND Location = ?
            """, row["Women"], row["Men"], row["Age Group"], row["Location"])
        else:
            # Insert new
            cursor.execute("""
                INSERT INTO dbo.QualifyingTimes (AgeGroup, Women, Men, Location)
                VALUES (?, ?, ?, ?)
            """, row["Age Group"], row["Women"], row["Men"], row["Location"])


def query_top_times(cursor, location=None, limit=5):
    """
    Query top qualifying times, optionally filtered by location.
    """
    if location:
        cursor.execute("""
            SELECT TOP (?) AgeGroup, Women, Men, Location 
            FROM dbo.QualifyingTimes 
            WHERE Location = ? 
            ORDER BY AgeGroup
        """, limit, location)
    else:
        cursor.execute("""
            SELECT TOP (?) AgeGroup, Women, Men, Location 
            FROM dbo.QualifyingTimes 
            ORDER BY Location, AgeGroup
        """, limit)
    return cursor.fetchall()
