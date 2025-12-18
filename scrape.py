# Import required libraries
import time
import hashlib
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from typing import Optional, Tuple, List

# Import shared configuration and functions from other scripts
from config import logger

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


def _get(url: str, retries: int = 3, backoff: float = 1.5, timeout: int = 15) -> requests.Response:
    """Definition for requests.get with simple retry and backoff."""
    last_exception = None
    session = requests.Session()
    session.headers.update(HEADERS)

    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as exception:
            last_exception = exception
            if attempt < retries:
                sleep_seconds = backoff ** attempt
                logger.warning(f"Failed to GET {url} ({exception}); retrying in {sleep_seconds:.1f}s (attempt {attempt}/{retries}).")
                time.sleep(sleep_seconds)

    logger.error(f"Failed to fetch {url}: {last_exception}")
    raise last_exception


def _parse_time_to_seconds(txt: Optional[str]) -> Optional[int]:
    """Accepts values like '3:10:00', 'sub 3:10:00' and '3:10', returns seconds or None."""
    if not txt:
        return None
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


def _normalise_table_rows(table: Optional[object]) -> List[List[str]]:
    """Helper to get rows for different table structures safely."""
    rows = []
    if table is None:
        return rows
    
    for tr in table.find_all("tr"):
        if tr.find_all("th"):
            continue
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if tds:
            rows.append(tds)
    
    return rows


def scrape_london() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Scrape the London Marathon qualifying info and times."""
    url = "https://www.londonmarathonevents.co.uk/london-marathon/good-age-entry"
    response = _get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    page_hash = hashlib.sha256(response.content).hexdigest()

    # Get race data
    logger.info("Parsing qualifying text and links for London Marathon")
    qualifying_text = "Not found"

    candidates = [
        "div.paragraph--type--inset-text div.col-md-start-7 p:nth-of-type(2)",
        "div.paragraph--type--inset-text p:nth-of-type(2)",
        "main p"
    ]

    for sel in candidates:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            qualifying_text = el.get_text(strip=True)
            break

    # Link to AIMS/worldrunning
    link_elem = soup.find("a", href=lambda href: href and ("aims-worldrunning" in href or "worldrunning" in href))
    link_text, link_url = (
        (link_elem.get_text(strip=True), link_elem['href']) if link_elem else ("Not found", url)
    )

    df_racedata = pd.DataFrame([{
        "RaceYear": datetime.now().year + 1,
        "Location": "London",
        "QualifyingText": qualifying_text,
        "LinkText": link_text,
        "LinkURL": link_url,
        "ScrapeDate": datetime.now(timezone.utc),
        "PageHash": page_hash,
    }])

    # Get qualifying details
    logger.info("Parsing age group qualifying times for London")
    london_table = None
    for tbl in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if any("age" in h for h in headers) and any("men" in h for h in headers) and any("women" in h for h in headers):
            london_table = tbl
            break

    if london_table is None:
        logger.error("Failed to find age group table in London Marathon page.")
        raise ValueError("Age group table missing (London)")

    # Determine order for men and women columns
    header_texts = [th.get_text(strip=True).lower() for th in london_table.find_all("th")]

    # Set default indices
    women_idx = None
    men_idx = None
    for idx, h in enumerate(header_texts):
        if "women" in h:
            women_idx = idx
        if "men" in h:
            men_idx = idx

    # Fallback defaults
    if women_idx is None or men_idx is None:
        women_idx, men_idx = 2, 1

    rows = _normalise_table_rows(london_table)
    parsed = []
    for cols in rows:
        if len(cols) <= max(women_idx, men_idx):
            continue
        age = cols[0]
        women = cols[women_idx]
        men = cols[men_idx]
        parsed.append((age, women, men))

    df_times = pd.DataFrame(parsed, columns=["Age Group", "Women", "Men"])
    df_times["WomenSeconds"] = df_times["Women"].apply(_parse_time_to_seconds)
    df_times["MenSeconds"] = df_times["Men"].apply(_parse_time_to_seconds)
    df_times["Location"] = "London"

    return df_racedata, df_times


def scrape_boston() -> Tuple[pd.DataFrame, pd.DataFrame]:
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
        if any("age" in h for h in headers) and ("men" in headers and "women" in headers):
            boston_table = tbl
            break

    if boston_table is None:
        logger.error("Failed to find Boston qualifying times table.")
        raise ValueError("Boston qualifying table missing")

    header_texts = [th.get_text(strip=True).lower() for th in boston_table.find_all("th")]

    # Determine order of men and women
    try:
        men_index = header_texts.index("men")
        women_index = header_texts.index("women")
        men_first = men_index < women_index
    except ValueError:
        men_first = True

    rows = _normalise_table_rows(boston_table)
    parsed = []
    for cols in rows:
        if len(cols) < 3:
            continue
        age = cols[0]
        if men_first:
            men = cols[1]
            women = cols[2]
        else:
            women = cols[1]
            men = cols[2]
        parsed.append({"Age Group": age, "Women": women, "Men": men})

    df_times = pd.DataFrame(parsed)
    df_times["WomenSeconds"] = df_times["Women"].apply(_parse_time_to_seconds)
    df_times["MenSeconds"] = df_times["Men"].apply(_parse_time_to_seconds)
    df_times["Location"] = "Boston"
    df_times = df_times[["Age Group", "Women", "Men", "Location", "WomenSeconds", "MenSeconds"]]

    # Basic race info text, kept from page headline snippets
    race_info = "Qualifier registration information (B.A.A.)"
    qual_window = "See B.A.A. site for qualifying window details."

    df_racedata = pd.DataFrame([{
        "RaceYear": datetime.now().year + 1 if datetime.now().month < 9 else datetime.now().year + 1,
        "Location": "Boston",
        "QualifyingText": qual_window,
        "LinkText": race_info,
        "LinkURL": url,
        "ScrapeDate": datetime.now(timezone.utc),
        "PageHash": page_hash
    }])

    return df_racedata, df_times


def scrape_tokyo():
    url = "https://www.marathon.tokyo/en/participants/run-as-one/"
    response = _get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    page_hash = hashlib.sha256(response.content).hexdigest()

    logger.info("Parsing qualifying text and links for Tokyo Marathon")


def scrape_new_york():
    url = "https://www.nyrr.org/tcsnycmarathon/runners/marathon-time-qualifiers"
    response = _get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    page_hash = hashlib.sha256(response.content).hexdigest()

    logger.info("Parsing qualifying text and links for New York Marathon")

def scrape_chicago():
    url = "https://www.chicagomarathon.com/apply/"
    response = _get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    page_hash = hashlib.sha256(response.content).hexdigest()

    logger.info("Parsing qualifying text and links for Chicago Marathon")

  