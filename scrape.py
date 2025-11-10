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

def _get(url, retries=3, backoff=1.5, timeout=15):
    """Definition for requests.get with simple retry and backoff."""
    last_e = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            last_e = e
            if attempt < retries:
                sleep_s = backoff ** attempt
                logger.warning(f"GET {url} failed ({e}), retrying in {sleep_s:.1f}s.")
                time.sleep(sleep_s)
    logger.error(f"Failed to fetch {url}: {last_e}")
    raise last_e


def _parse_time_to_seconds(txt):
    t = txt.strip().lower().replace("sub", "").strip()
    parts = t.split(":")
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
    logger.info("Parsing London Marathon qualifying text and links")
    qualifying_period = soup.select_one("div.paragraph--type--inset-text div.col-md-start-7 p:nth-of-type(2)")
    qualifying_text = qualifying_period.get_text(strip=True) if qualifying_period else "Not found"

    link_elem = soup.find("a", href=lambda href: href and "aims-worldrunning" in href)
    link_text, link_url = (
        (link_elem.get_text(strip=True), link_elem['href']) if link_elem else ("Not found", "Not found")
    )

    df_racedata = pd.DataFrame([{
        "RaceYear": datetime.now().year + 1,
        "Location": "London",
        "QualifyingText": qualifying_text,
        "LinkText": link_text,
        "LinkURL": link_url,
        "ScrapeDate": datetime.now(timezone.utc),
    }])

    # Get qualifying details
    logger.info("Parsing age group qualifying times")
    age_group_div = soup.select_one("body > div.dialog-off-canvas-main-canvas > div > main > section:nth-child(6) > div")
    if not age_group_div:
        logger.error("Failed to find age group section in London Marathon page.")
        raise ValueError("Age group section missing")

    london_age_rows = [
        [td.get_text(strip=True) for td in row.select("td")]
        for row in age_group_div.select("tbody tr") if len(row.select("td")) == 3
    ]

    df_times = pd.DataFrame(london_age_rows, columns=["Age Group", "Women", "Men"])
    df_times["Location"] = "London"
    return df_racedata, df_times


def scrape_boston():
    """Scrape the Boston Marathon qualifying info and times."""
    url = "https://www.baa.org/races/boston-marathon/qualify"
    response = _get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    page_hash = hashlib.sha256(response.content).hexdigest()

    logger.info("Parsing Boston Marathon qualifying table")

    # Identify the table with age group, men and women headers
    boston_table = None
    for tbl in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if any("age" in h for h in headers) and (("men" in headers and "women" in headers) or ("women" in headers and "men" in headers)):
            boston_table = tbl
            break

    if not boston_table:
        logger.error("Failed to find Boston qualifying times table.")
        raise ValueError("Boston qualifying table missing")

    boston_data = []
    header_order = [th.get_text(strip=True).lower() for th in boston_table.find_all("th")]
    men_first = header_order and header_order.index("men") < header_order.index("women") if ("men" in header_order and "women" in header_order) else True

    for row in boston_table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) >= 3:
            age = cells[0]
            if men_first:
                men, women = cells[1], cells[2]
            else:
                women, men = cells[1], cells[2]
            boston_data.append({
                "Age Group": age,
                "Women": women,
                "Men": men
            })

    df_times = pd.DataFrame(boston_data)

    # Normalise to seconds for reliable comparisons
    df_times["WomenSeconds"] = df_times["Women"].apply(_parse_time_to_seconds)
    df_times["MenSeconds"] = df_times["Men"].apply(_parse_time_to_seconds)
    df_times["Location"] = "Boston"
    df_times = df_times[["Age Group", "Women", "Men", "Location", "WomenSeconds", "MenSeconds"]]

    # Basic race info text, kept from page headline snippets
    race_info = "Qualifier registration will be held within the B.A.A.’s online platform Athletes' Village between September 8–12, 2025"
    qual_window = "The 2026 Boston Marathon qualifying window began on September 1, 2024, and will close at 5:00 p.m. ET on Friday, September 12"

    df_racedata = pd.DataFrame([{
        "RaceYear": 2026,
        "Location": "Boston",
        "QualifyingText": qual_window,
        "LinkText": race_info,
        "LinkURL": url,
        "ScrapeDate": datetime.now(timezone.utc),
        "PageHash": page_hash
    }])

    return df_racedata, df_times
