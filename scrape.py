# Import required libraries
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

def scrape_london():
    """
    Scrape the London Marathon qualifying info and times.
    """
    url = "https://www.londonmarathonevents.co.uk/london-marathon/good-age-entry"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
    except requests.RequestException as e:
        logger.error(f"Failed to fetch London Marathon page: {e}")
        raise

    # Get race data
    logger.info("Parsing London Marathon qualifying text and links...")
    qualifying_period = soup.select_one("div.paragraph--type--inset-text div.col-md-start-7 p:nth-of-type(2)")
    qualifying_text = qualifying_period.get_text(strip=True) if qualifying_period else "Not found"

    link_elem = soup.find("a", href=lambda href: href and "aims-worldrunning" in href)
    link_text = link_elem.get_text(strip=True) if link_elem else "Not found"
    link_url = link_elem['href'] if link_elem else "Not found"

    df_racedata = pd.DataFrame([{
        "RaceYear": pd.to_datetime("today").year + 1,
        "Location": "London",
        "QualifyingText": qualifying_text,
        "LinkText": link_text,
        "LinkURL": link_url,
        "ScrapeDate": datetime.now(timezone.utc)
    }])

    # Get qualifying details
    logger.info("Parsing age group qualifying times...")
    age_group_div = soup.select_one("body > div.dialog-off-canvas-main-canvas > div > main > section:nth-child(5) > div")
    if not age_group_div:
        logger.error("Failed to find age group section in London Marathon page.")
        raise ValueError("Age group section missing")

    raw_text = age_group_div.get_text(separator="|", strip=True)
    data_parts = raw_text.split("|")[4:]
    london_age_rows = [data_parts[i:i+3] for i in range(0, len(data_parts), 3)]

    df_times = pd.DataFrame(london_age_rows, columns=["Age Group", "Women", "Men"])
    df_times["Location"] = "London"
    df_times = df_times[["Age Group", "Women", "Men", "Location"]]

    return df_racedata, df_times


def scrape_boston():
    """
    Scrape the Boston Marathon qualifying info and times.
    """
    url = "https://www.baa.org/races/boston-marathon/qualify"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
    except requests.RequestException as e:
        logger.error(f"Failed to fetch Boston Marathon page: {e}")
        raise

    boston_table = soup.find("table")
    rows = boston_table.find_all("tr")

    boston_data = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= 3:
            boston_data.append({
                "Age Group": cells[0].get_text(strip=True),
                "Men": cells[1].get_text(strip=True),
                "Women": cells[2].get_text(strip=True)
            })

    df_times = pd.DataFrame(boston_data)
    df_times["Location"] = "Boston"
    df_times = df_times[["Age Group", "Women", "Men", "Location"]]

    race_info = "Qualifier registration will be held within the B.A.A.’s online platform Athletes' Village between September 8–12, 2025"
    qual_window = "The 2026 Boston Marathon qualifying window began on September 1, 2024, and will close at 5:00 p.m. ET on Friday, September 12"

    df_racedata = pd.DataFrame([{
        "RaceYear": 2026,
        "Location": "Boston",
        "QualifyingText": qual_window,
        "LinkText": race_info,
        "LinkURL": "",
        "ScrapeDate": datetime.now(timezone.utc)
    }])

    return df_racedata, df_times
