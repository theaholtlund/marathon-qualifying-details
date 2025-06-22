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
