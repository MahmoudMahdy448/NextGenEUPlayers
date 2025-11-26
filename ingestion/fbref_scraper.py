import cloudscraper
import pandas as pd
import os
import time
import re
from bs4 import BeautifulSoup
from io import StringIO

# Constants
BASE_URL = "https://fbref.com"
SEASONS = ["2023-2024", "2024-2025", "2025-2026"]
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")

# The 11 specific tables we want to scrape
TARGET_TABLES = [
    "Standard Stats",
    "Goalkeeping",
    "Advanced Goalkeeping",
    "Shooting",
    "Passing",
    "Pass Types",
    "Goal and Shot Creation",
    "Defensive Actions",
    "Possession",
    "Playing Time",
    "Miscellaneous Stats"
]

def get_soup(scraper, url):
    """Fetches a URL using cloudscraper and returns a BeautifulSoup object."""
    try:
        print(f"Requesting URL: {url}")
        response = scraper.get(url)
        response.raise_for_status()
        return BeautifulSoup(response.content, "lxml")
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def clean_filename(name):
    """Cleans a string to be used as a filename."""
    return re.sub(r'[\\/*?:"<>|]', "", name).lower().replace(" ", "_")

def get_season_url(season):
    """Constructs the Big 5 URL for a specific season."""
    # 2025-2026 is the current season, so it has a different URL structure or is the default
    # However, Fbref usually has a specific ID for each season.
    # We will start from the main page and find the season links if possible, 
    # OR construct them if we know the IDs. 
    # For simplicity and robustness, we'll use the history page or known IDs if available.
    # BUT, for Big 5, the URL usually looks like:
    # https://fbref.com/en/comps/Big5/{season}/Big-5-European-Leagues-Stats
    return f"https://fbref.com/en/comps/Big5/{season}/Big-5-European-Leagues-Stats"

def scrape_season(scraper, season):
    """Scrapes all target tables for a specific season."""
    print(f"\n=== Starting Scrape for Season: {season} ===")
    
    season_dir = os.path.join(DATA_DIR, season)
    os.makedirs(season_dir, exist_ok=True)
    
    url = get_season_url(season)
    soup = get_soup(scraper, url)
    
    if not soup:
        print(f"Could not load page for season {season}")
        return

    # Find links to the player stats tables
    links_to_scrape = []
    all_links = soup.find_all("a", href=True)
    
    for link in all_links:
        text = link.get_text().strip()
        if text in TARGET_TABLES:
            href = link['href']
            # Crucial fix: Ensure we are getting PLAYER stats, not SQUAD stats.
            # Fbref links for players usually contain 'players' in the path.
            # Squad links often contain 'squads'.
            if "squads" not in href:
                 full_url = BASE_URL + href
                 if (text, full_url) not in links_to_scrape:
                     links_to_scrape.append((text, full_url))

    print(f"Found {len(links_to_scrape)} tables for {season}.")

    for table_name, table_url in links_to_scrape:
        print(f"  - Processing: {table_name}...")
        
        # Rate limiting
        time.sleep(4) 
        
        try:
            response = scraper.get(table_url)
            response.raise_for_status()
            
            # Use StringIO to avoid pandas warning
            dfs = pd.read_html(StringIO(response.text))
            
            if dfs:
                df = dfs[0]
                # Flatten multi-level columns if present
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = ['_'.join(col).strip() for col in df.columns.values]
                
                safe_name = clean_filename(table_name)
                # Naming convention: table_name_season.csv
                filename = f"{safe_name}_{season}.csv"
                filepath = os.path.join(season_dir, filename)
                
                df.to_csv(filepath, index=False)
                print(f"    Saved to {filepath}")
            else:
                print(f"    No tables found for {table_name}")

        except Exception as e:
            print(f"    Failed to scrape {table_name}: {e}")

def main():
    # Initialize cloudscraper with proxy support
    proxy_url = os.environ.get("PROXY_URL")
    scraper_kwargs = {
        'browser': {'browser': 'firefox', 'platform': 'windows', 'mobile': False}
    }
    
    if proxy_url:
        print(f"Using proxy: {proxy_url}")
        scraper = cloudscraper.create_scraper(**scraper_kwargs)
        scraper.proxies = {
            'http': proxy_url,
            'https': proxy_url
        }
    else:
        print("No PROXY_URL found. Using direct connection.")
        scraper = cloudscraper.create_scraper(**scraper_kwargs)

    for season in SEASONS:
        scrape_season(scraper, season)

if __name__ == "__main__":
    main()
