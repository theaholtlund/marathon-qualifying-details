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
    """Scrape website data regarding race information and qualifying times for London Marathon."""
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

    # Fint link for AIMS website
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
    women_idx = next((i for i, h in enumerate(header_texts) if "women" in h), 2)
    men_idx = next((i for i, h in enumerate(header_texts) if "men" in h), 1)

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
        if len(cols) <= max(age_idx, men_idx, women_idx):
            continue

        records.append({
            "Age Group": cols[age_idx],
            "Women": cols[women_idx],
            "Men": cols[men_idx],
            "Location": "Boston"
        })

    df_times = pd.DataFrame(records)

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

    print("Boston qualifying times: ", df_times)

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

    tables = soup.find_all("table")
    if len(tables) < 2:
        raise ValueError("Unexpected NYC Marathon table structure")

    men_rows = _normalise_table_rows(tables[0])
    women_rows = _normalise_table_rows(tables[1])

    records = []
    for m, w in zip(men_rows, women_rows):
        # Skip headers
        if "age" in m[0].lower():
            continue
        records.append({
            "Age Group": m[0],
            "Women": w[1],
            "Men": m[1],
            "Location": "New York"
        })

    df_times = pd.DataFrame(records)

    df_racedata = pd.DataFrame([{
        "RaceYear": datetime.now().year + 1,
        "Location": "New York",
        "QualifyingText": "Time qualifiers",
        "LinkText": "NYRR",
        "LinkURL": url,
        "ScrapeDate": datetime.now(timezone.utc),
        "PageHash": page_hash,
    }])

    print("New York qualifying times: ", df_times)

    return df_racedata, df_times


def scrape_chicago() -> Tuple[pd.DataFrame, pd.DataFrame]:
    url = "https://www.chicagomarathon.com/apply/"
    response = _get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    page_hash = hashlib.sha256(response.content).hexdigest()

    logger.info("Parsing qualifying text and links for Chicago Marathon")

    table = soup.find("table")
    rows = _normalise_table_rows(table)
    records = []
    for cols in rows:
        records.append({ 
            "Age Group": cols[0],
            "Women": cols[2],
            "Men": cols[1],
            "Location": "Chicago"})
        
        df_times = pd.DataFrame(records)
        df_racedata = pd.DataFrame([{ 
            "RaceYear": datetime.now().year + 1, 
            "Location": "Chicago", 
            "QualifyingText": "Time qualifier standards", 
            "LinkText": "Chicago Marathon", 
            "LinkURL": url, 
            "ScrapeDate": datetime.now(timezone.utc), 
            "PageHash": page_hash, }]) 
        
        return df_racedata, df_times

def scrape_berlin() -> Tuple[pd.DataFrame, pd.DataFrame]:
    url = "https://www.bmw-berlin-marathon.com/en/registration/lottery"
    response = _get(url)
    page_hash = hashlib.sha256(response.content).hexdigest()

    data = [
        ("18–44", "3:10:00", "2:45:00"),
        ("45–59", "3:30:00", "2:55:00"),
        ("60+", "4:20:00", "3:25:00"),
    ]

    df_times = pd.DataFrame(data, columns=["Age Group", "Women", "Men"])
    df_times["Location"] = "Berlin"

    df_racedata = pd.DataFrame([{
        "RaceYear": datetime.now().year + 1,
        "Location": "Berlin",
        "QualifyingText": "Qualifying standards",
        "LinkText": "Berlin Marathon",
        "LinkURL": url,
        "ScrapeDate": datetime.now(timezone.utc),
        "PageHash": page_hash,
    }])

    print("Berlin qualifying times: ", df_times)

    return df_racedata, df_times