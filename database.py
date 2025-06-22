def create_tables(cursor):
    """
    Create tables RaceData and QualifyingTimes if they do not exist.
    RaceData stores race year, location, qualifying text and reference links.
    QualifyingTimes stores qualifying times by age group and location.
    """
    cursor.execute("""
    IF OBJECT_ID('dbo.Metadata', 'U') IS NULL
    CREATE TABLE dbo.Metadata (
        RaceYear INT,
        Location NVARCHAR(50),
        QualifyingText NVARCHAR(MAX),
        LinkText NVARCHAR(255),
        LinkURL NVARCHAR(255)
    );
    """)

    cursor.execute("""
    IF OBJECT_ID('dbo.QualifyingTimes', 'U') IS NULL
    CREATE TABLE dbo.QualifyingTimes (
        AgeGroup NVARCHAR(20),
        Women NVARCHAR(20),
        Men NVARCHAR(20),
        Location NVARCHAR(50)
    );
    """)

def insert_racedata(cursor, df_racedata):
    """
    Insert rows from race data DataFrame into RaceData table.
    """
    for _, row in df_racedata.iterrows():
        cursor.execute(
            "INSERT INTO dbo.RaceData (RaceYear, Location, QualifyingText, LinkText, LinkURL) VALUES (?, ?, ?, ?, ?)",
            row.RaceYear, row.Location, row.QualifyingText, row.LinkText, row.LinkURL
        )

def insert_qualifying_times(cursor, df_times):
    """
    Insert rows from qualifying times DataFrame into QualifyingTimes table.
    """
    for _, row in df_times.iterrows():
        cursor.execute(
            "INSERT INTO dbo.QualifyingTimes (AgeGroup, Women, Men, Location) VALUES (?, ?, ?, ?)",
            row["Age Group"], row.Women, row.Men, row.Location
        )

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
