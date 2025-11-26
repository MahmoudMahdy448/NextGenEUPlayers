import cloudscraper
import pandas as pd
import os
import time
import re
from bs4 import BeautifulSoup

# Constants
BASE_URL = "https://fbref.com"
# We only need to scrape one season to get the glossary, as definitions rarely change between seasons.
SEASON = "2025-2026" 
BIG5_URL = f"https://fbref.com/en/comps/Big5/{SEASON}/Big-5-European-Leagues-Stats"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "glossary")

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

def scrape_glossary():
    """Scrapes column definitions from the table headers."""
    
    os.makedirs(DATA_DIR, exist_ok=True)
    
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

    print(f"Fetching main page: {BIG5_URL}")
    soup = get_soup(scraper, BIG5_URL)
    if not soup:
        return

    # Find links to the player stats tables
    links_to_scrape = []
    all_links = soup.find_all("a", href=True)
    
    # Find links to the player stats tables
    links_to_scrape = []
    seen_urls = set()
    all_links = soup.find_all("a", href=True)
    
    for link in all_links:
        text = link.get_text().strip()
        if text in TARGET_TABLES:
            href = link['href']
            # Ensure we get PLAYER stats, not SQUAD stats
            if "squads" not in href:
                 full_url = BASE_URL + href
                 if full_url not in seen_urls:
                     links_to_scrape.append((text, full_url))
                     seen_urls.add(full_url)

    print(f"Found {len(links_to_scrape)} unique tables to inspect.")
    
    all_definitions = []

    for table_name, table_url in links_to_scrape:
        print(f"  - Inspecting: {table_name}...")
        
        # Rate limiting
        time.sleep(4)
        
        try:
            soup = get_soup(scraper, table_url)
            if not soup:
                continue
                
            # Find the stats table
            tables = soup.find_all("table")
            target_table = None
            
            # Heuristic: Look for stats_table class
            for t in tables:
                if "stats_table" in t.get("class", []):
                    target_table = t
                    break
            
            if not target_table:
                # Fallback: Try to find table by ID based on table name (approximate)
                # e.g. Standard Stats -> stats_standard_...
                print(f"    Warning: No 'stats_table' class found. Trying heuristic...")
                if tables:
                    target_table = tables[0] # Often the first table is the one we want if it's a specific page
            
            if not target_table:
                print(f"    Error: Could not find any table for {table_name}")
                continue

            # Extract headers and tooltips
            thead = target_table.find("thead")
            if not thead:
                print(f"    Error: No header found for {table_name}")
                continue
                
            # Iterate through ALL header rows to find definitions
            # Sometimes definitions are in the top row (categories) or bottom row (metrics)
            rows = thead.find_all("tr")
            found_count = 0
            
            for row in rows:
                for th in row.find_all("th"):
                    col_name = th.get_text().strip()
                    definition = th.get("data-tip") or th.get("title")
                    
                    if not definition:
                        child_with_tip = th.find(attrs={"data-tip": True})
                        if child_with_tip:
                            definition = child_with_tip.get("data-tip")
                    
                    if definition:
                        # Clean up definition
                        definition = re.sub(r'<[^>]+>', '', definition)
                        definition = definition.replace("&nbsp;", " ")
                        
                        # Avoid duplicates for the same column in the same table
                        # (Sometimes headers are repeated)
                        if col_name and definition:
                            # Check if we already have this col for this table
                            exists = any(d['table_name'] == table_name and d['column_name'] == col_name for d in all_definitions)
                            if not exists:
                                all_definitions.append({
                                    "table_name": table_name,
                                    "column_name": col_name,
                                    "definition": definition
                                })
                                found_count += 1
            
            print(f"    Extracted {found_count} definitions.")

        except Exception as e:
            print(f"    Error processing {table_name}: {e}")
                
    # Save to CSV
    if all_definitions:
        df = pd.DataFrame(all_definitions)
        output_path = os.path.join(DATA_DIR, "glossary.csv")
        df.to_csv(output_path, index=False)
        print(f"\nSaved glossary to {output_path}")
        print(df.head())
    else:
        print("\nNo definitions found.")

if __name__ == "__main__":
    scrape_glossary()
