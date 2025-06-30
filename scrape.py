# Import required libraries
import requests
import pandas as pd
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

def scrape_london():
    url = "https://www.londonmarathonevents.co.uk/london-marathon/good-age-entry"
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")

    qualifying_period_elem = soup.select_one("div.paragraph--type--inset-text div.col-md-start-7 p:nth-of-type(2)")
    qualifying_text = qualifying_period_elem.get_text(strip=True) if qualifying_period_elem else "Not found"

    aims_link_elem = soup.find("a", href=lambda href: href and "aims-worldrunning" in href)
    aims_link_text = aims_link_elem.get_text(strip=True) if aims_link_elem else "Not found"
    aims_link_url = aims_link_elem['href'] if aims_link_elem else "Not found"

    df_racedata = pd.DataFrame([{
        "RaceYear": 2026,
        "Location": "London",
        "QualifyingText": qualifying_text,
        "LinkText": aims_link_text,
        "LinkURL": aims_link_url
    }])

    age_group_div = soup.select_one("body > div.dialog-off-canvas-main-canvas > div > main > section:nth-child(5) > div")
    raw_text = age_group_div.get_text(separator="|", strip=True)
    data_parts = raw_text.split("|")[4:]
    london_age_rows = [data_parts[i:i+3] for i in range(0, len(data_parts), 3)]

    df_times = pd.DataFrame(london_age_rows, columns=["Age Group", "Women", "Men"])
    df_times["Location"] = "London"
    df_times = df_times[["Age Group", "Women", "Men", "Location"]]

    return df_racedata, df_times
