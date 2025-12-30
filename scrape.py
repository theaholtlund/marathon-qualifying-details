# Import required libraries
import re
import time
import hashlib
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Optional, Tuple, List

# Import shared configuration and functions from other scripts
from config import logger

# Standard headers for HTTP requests
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
    if table is None:
        return []
    rows = []
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
    link_text, link_url = (link_elem.get_text(strip=True), link_elem['href']) if link_elem else ("Not found", url)

    # Prepare race metadata
    df_racedata = pd.DataFrame([{
        "RaceYear": datetime.now().year + 1,
        "Location": "London",
        "QualifyingText": qualifying_text,
        "LinkText": link_text,
        "LinkURL": link_url,
        "ScrapeDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "PageHash": page_hash,
    }])

    # Get qualifying details
    london_table = None
    for tbl in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if all(x in " ".join(headers) for x in ["age", "men", "women"]):
            london_table = tbl
            break

    if london_table is None:
        logger.error("Failed to find age group table for London Marathon")
        raise ValueError("London age group table missing")

    # Determine order for men and women columns
    header_texts = [th.get_text(strip=True).lower() for th in london_table.find_all("th")]
    women_idx = next((i for i, h in enumerate(header_texts) if "women" in h), 2)
    men_idx = next((i for i, h in enumerate(header_texts) if "men" in h), 1)

    # Parse table rows
    parsed = [(cols[0], cols[women_idx], cols[men_idx]) 
              for cols in _normalise_table_rows(london_table) 
              if len(cols) > max(women_idx, men_idx)]

    df_times = pd.DataFrame(parsed, columns=["Age Group", "Women", "Men"])
    df_times["Location"] = "London"

    print("London qualifying times: ", df_times)

    return df_racedata, df_times


def scrape_boston() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Scrape website data regarding race information and qualifying times for Boston Marathon."""
    url = "https://www.baa.org/races/boston-marathon/qualify"
    response = _get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    page_hash = hashlib.sha256(response.content).hexdigest()

    logger.info("Parsing qualifying text and links for Boston Marathon")
    
    # Locate table with qualifying times
    boston_table = next((
        tbl for tbl in soup.find_all("table")
        if all(any(x in th.get_text(strip=True).lower() for th in tbl.find_all("th")) for x in ["age", "men", "women"])
    ), None)

    if boston_table is None:
        logger.error("Failed to find age group table for Boston Marathon")
        raise ValueError("Boston age group table missing")

    # Identify column indices
    headers = [th.get_text(strip=True).lower() for th in boston_table.find_all("th")]
    age_idx = next(i for i, h in enumerate(headers) if "age" in h)
    men_idx = next(i for i, h in enumerate(headers) if "men" in h)
    women_idx = next(i for i, h in enumerate(headers) if "women" in h)

    # Parse table rows
    records = [
        {"Age Group": cols[age_idx], "Women": cols[women_idx], "Men": cols[men_idx], "Location": "Boston"}
        for cols in _normalise_table_rows(boston_table)
        if len(cols) > max(age_idx, men_idx, women_idx)
    ]

    df_times = pd.DataFrame(records)

    # Prepare race metadata
    df_racedata = pd.DataFrame([{
        "RaceYear": datetime.now().year + 1,
        "Location": "Boston",
        "QualifyingText": "See B.A.A. site for qualifying window details.",
        "LinkText": "Qualifier registration information (B.A.A.)",
        "LinkURL": url,
        "ScrapeDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "PageHash": page_hash,
    }])

    print("Boston qualifying times: ", df_times)

    return df_racedata, df_times


def scrape_tokyo() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Scrape website data regarding race information and qualifying times for Tokyo Marathon."""
    url = "https://www.marathon.tokyo/en/participants/run-as-one/"
    response = _get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    page_hash = hashlib.sha256(response.content).hexdigest()

    logger.info("Parsing qualifying text and links for Tokyo Marathon")

    men_time = women_time = None
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        if "qualifying times" not in tds[0].get_text(strip=True).lower():
            continue

        text = tds[1].get_text(" ", strip=True)
        men_match = re.search(r"men.*?(\d{1,2})hrs (\d{1,2})min (\d{2})sec", text, re.IGNORECASE)
        women_match = re.search(r"women.*?(\d{1,2})hrs (\d{1,2})min (\d{2})sec", text, re.IGNORECASE)

        if men_match:
            men_time = f"{int(men_match[1]):02d}:{int(men_match[2]):02d}:{int(men_match[3]):02d}"
        if women_match:
            women_time = f"{int(women_match[1]):02d}:{int(women_match[2]):02d}:{int(women_match[3]):02d}"

        if men_time and women_time:
            break

    if not men_time or not women_time:
        logger.error("Failed to find age group table for Tokyo Marathon")
        raise ValueError("Tokyo age group table missing")

    df_times = pd.DataFrame([{"Age Group": "18+", "Women": women_time, "Men": men_time, "Location": "Tokyo"}])

    # Prepare race metadata
    df_racedata = pd.DataFrame([{
        "RaceYear": datetime.now().year + 1,
        "Location": "Tokyo",
        "QualifyingText": "Elite qualifying standards (Run as One)",
        "LinkText": "Tokyo Marathon – Run as One",
        "LinkURL": url,
        "ScrapeDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "PageHash": page_hash,
    }])

    print("Tokyo qualifying times: ", df_times)

    return df_racedata, df_times


def scrape_new_york() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Scrape website data regarding race information and qualifying times for New York Marathon."""
    url = "https://www.nyrr.org/tcsnycmarathon/runners/marathon-time-qualifiers"
    response = _get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    page_hash = hashlib.sha256(response.content).hexdigest()

    logger.info("Parsing qualifying text and links for New York Marathon")

    tables = soup.find_all("table")
    if len(tables) < 2:
        raise ValueError("Unexpected New York Marathon table structure")

    men_rows, women_rows = _normalise_table_rows(tables[0]), _normalise_table_rows(tables[1])
    records = [
        {"Age Group": m[0], "Women": w[1], "Men": m[1], "Location": "New York"}
        for m, w in zip(men_rows, women_rows) if "age" not in m[0].lower()
    ]

    df_times = pd.DataFrame(records)

    # Prepare race metadata
    df_racedata = pd.DataFrame([{
        "RaceYear": datetime.now().year + 1,
        "Location": "New York",
        "QualifyingText": "Time qualifiers",
        "LinkText": "NYRR",
        "LinkURL": url,
        "ScrapeDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "PageHash": page_hash,
    }])

    print("New York qualifying times: ", df_times)

    return df_racedata, df_times


def scrape_chicago() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Scrape website data regarding race information and qualifying times for Chicago Marathon."""
    url = "https://www.chicagomarathon.com/apply/"
    response = _get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    page_hash = hashlib.sha256(response.content).hexdigest()

    logger.info("Parsing qualifying text and links for Chicago Marathon")

    rows = _normalise_table_rows(soup.find("table"))
    records = [
        {"Age Group": cols[0], "Men": cols[1], "Women": cols[2], "Location": "Chicago"}
        for cols in rows if len(cols) >= 3 and "age" not in cols[0].lower()
    ]

    df_times = pd.DataFrame(records)

    # Prepare race metadata
    df_racedata = pd.DataFrame([{
        "RaceYear": datetime.now().year + 1,
        "Location": "Chicago",
        "QualifyingText": "Time qualifier standards",
        "LinkText": "Chicago Marathon",
        "LinkURL": url,
        "ScrapeDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "PageHash": page_hash,
    }])

    print("Chicago qualifying times: ", df_times)

    return df_racedata, df_times


def scrape_berlin() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Scrape website data regarding race information and qualifying times for Berlin Marathon."""
    url = "https://www.bmw-berlin-marathon.com/en/registration/lottery"
    response = _get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    page_hash = hashlib.sha256(response.content).hexdigest()

    logger.info("Parsing qualifying text and links for Berlin Marathon")

    # Begin scraping the qualifying times
    ul_tags = soup.find_all("ul", class_="list-normal")

    # Find the male and female times
    target_ul = None
    for ul in ul_tags:
        text = ul.get_text(separator="\n").strip()
        if "Male" in text and "Female" in text:
            target_ul = ul
            break

    # Extract lines containing qualifying times
    li_tags = target_ul.find_all("li")
    male_lines, female_lines = [], []
    for br_or_text in li_tags[0].children:
        if hasattr(br_or_text, "get_text"):
            text = br_or_text.get_text(strip=True)
        elif isinstance(br_or_text, str):
            text = br_or_text.strip()
        else:
            continue
        if re.search(r"under:?\s*\d+", text, re.IGNORECASE):
            male_lines.append(text)

    for br_or_text in li_tags[1].children:
        if hasattr(br_or_text, "get_text"):
            text = br_or_text.get_text(strip=True)
        elif isinstance(br_or_text, str):
            text = br_or_text.strip()
        else:
            continue
        if re.search(r"under:?\s*\d+", text, re.IGNORECASE):
            female_lines.append(text)

    # Parse times manually
    def parse_line_to_time(line):
        match = re.search(r"under:?\s*(\d+):(\d{2})", line)
        if match:
            hours, minutes = int(match.group(1)), int(match.group(2))
        else:
            match = re.search(r"under:?\s*(\d+)\s*hours", line, re.IGNORECASE)
            if match:
                hours, minutes = int(match.group(1)), 0
            else:
                raise ValueError(f"Could not parse time from: {line}")
        return f"{hours:02d}:{minutes:02d}:00"

    male_times = [parse_line_to_time(line) for line in male_lines]
    female_times = [parse_line_to_time(line) for line in female_lines]

    if len(male_times) != 3 or len(female_times) != 3:
        logger.error("Failed to find age group table for Berlin Marathon")
        raise ValueError("Berlin age group table missing")

    age_groups = ["0–44", "45–59", "60+"]
    df_times = pd.DataFrame([
        {"Age Group": age_groups[i], "Women": female_times[i], "Men": male_times[i], "Location": "Berlin"}
        for i in range(3)
    ])

    # Prepare race metadata
    df_racedata = pd.DataFrame([{
        "RaceYear": datetime.now().year + 1,
        "Location": "Berlin",
        "QualifyingText": "Official Berlin Marathon qualifying standards",
        "LinkText": "Berlin Marathon",
        "LinkURL": url,
        "ScrapeDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "PageHash": page_hash,
    }])

    print("Berlin qualifying times: ", df_times)

    return df_racedata, df_times
