#!/usr/bin/env python
import asyncio
import json
import os
import random
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Set
import logging
from curl_cffi.requests import AsyncSession
import datetime
import base64
from google.cloud import storage
from google.oauth2 import service_account

from dotenv import load_dotenv
load_dotenv()

# Set up environment-based paths
MODE = os.getenv('MODE', 'dev')


def load_gcp_credentials():
    try:
        credentials_b64 = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not credentials_b64:
            raise ValueError(f"Environment variable not found or empty")
        
        credentials_bytes = base64.b64decode(credentials_b64)
        credentials_dict = json.loads(credentials_bytes)
        return service_account.Credentials.from_service_account_info(credentials_dict)
    except Exception as e:
        logger.error(f"Failed to load credentials from environment: {e}")
        raise
    
# Set up environment-based logging
if MODE == 'prod':
    # Production: Use Cloud Logging + structured stdout
    try:
        from google.cloud import logging as cloud_logging

        # Load GCP credentials for logging
        credentials = load_gcp_credentials()
        cloud_client = cloud_logging.Client(credentials=credentials)
        cloud_client.setup_logging()
        print("✅ Google Cloud Logging integration enabled for AUCB scraper")

    except Exception as e:
        print(f"⚠️ Cloud Logging setup failed, using stdout only: {e}")
    
    # Use structured format for production
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )
else:
    # Development: Use rich console + local file
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('./scrape/aucb_scraper.log', mode="w"),
            logging.StreamHandler()
        ]
    )
logger = logging.getLogger(__name__)


PROXY = os.getenv('PROXY')
PREV_DATE = os.getenv('PREV_DATE', '2025-05-01')

if MODE == 'prod':
    # GCP Storage paths
    OUTPUT_BASE_DIR = 'cricket-data-1'
    JSON_DATA_DIR = 'json_data'
    
    # Load GCP credentials


    # Initialize GCP client
    try:
        credentials = load_gcp_credentials()
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(OUTPUT_BASE_DIR)
    except Exception as e:
        logger.error(f"Failed to initialize GCP storage: {e}")
        raise
else:
    # Local paths
    JSON_DATA_DIR = 'json_data'

# Helper functions for file operations
def write_file(file_path: str, data: dict) -> None:
    """Write data to either local storage or GCP bucket"""
    if MODE == 'prod':
        try:
            blob = bucket.blob(file_path)
            blob.upload_from_string(json.dumps(data, indent=4))
        except Exception as e:
            logger.error(f"Error writing to GCP: {file_path} - {e}")
            raise
    else:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)

def file_exists(file_path: str) -> bool:
    """Check if file exists in either local storage or GCP bucket"""
    if MODE == 'prod':
        try:
            blob = bucket.blob(file_path)
            return blob.exists()
        except Exception as e:
            logger.error(f"Error checking file existence in GCP: {file_path} - {e}")
            raise
    else:
        return os.path.exists(file_path)

def ensure_dir(dir_path: str) -> None:
    """Ensure directory exists (only needed for local storage)"""
    if MODE != 'prod':
        os.makedirs(dir_path, exist_ok=True)

# Constants
BASE_SCORECARD_URL = "https://apiv2.cricket.com.au/web/views/scorecard?fixtureId={}&jsconfig=eccn%3Atrue&format=json"
BASE_COMMENTS_URL = "https://apiv2.cricket.com.au/web/views/comments?fixtureId={}&inningNumber={}&commentType=&overLimit=499&jsconfig=eccn%3Atrue&format=json"
BASE_FIXTURES_URL = "https://apiv2.cricket.com.au/web/fixtures/yearfilter?isCompleted=true&isWomenMatch=false&year={year}&limit=999&isInningInclude=true&jsconfig=eccn%3Atrue&format=json"
OUTPUT_DIR = Path("json_data/aucb_matches")
CONCURRENCY_LIMIT = 300  # Adjust based on your needs
FIXTURES_CONCURRENCY_LIMIT = 5
FIXTURES_DELAY_SECONDS = 3
YEARS = range(2025, 2026)  # 2019 to 2025 inclusive
VALID_GAME_TYPE_IDS = {1, 2, 3, 6, 24}

# Webshare proxy configuration
PROXY_LIST = [
    {"http": PROXY.replace('{i}', str(i)), 
     "https": PROXY.replace('{i}', str(i))}
    for i in range(1, 101)
]

headers = {
    'Accept': 'application/json',
    'Referer': 'https://www.cricket.com.au/',
}

class FixtureScraper:
    """Class to handle scraping fixtures data"""
    
    @staticmethod
    async def fetch_fixtures(session: AsyncSession, year: int, semaphore: asyncio.Semaphore):
        """Fetches fixture data for a specific year."""
        url = BASE_FIXTURES_URL.format(year=year)
        async with semaphore:
            try:
                logger.info(f"Fetching fixtures for year: {year}")
                response = await session.get(url)
                response.raise_for_status()  # Raise an exception for bad status codes
                data = response.json()
                logger.info(f"Successfully fetched fixtures for year: {year}")
                await asyncio.sleep(FIXTURES_DELAY_SECONDS)  # Wait after successful fetch before releasing semaphore
                return data.get("fixtures", [])
            except Exception as e:
                logger.error(f"Error fetching fixtures for year {year}: {e}")
                await asyncio.sleep(FIXTURES_DELAY_SECONDS)  # Also wait on error before releasing
                return []  # Return empty list on error

    @staticmethod
    async def save_fixture(fixture: dict):
        """Saves a single fixture to its specific JSON file."""
        fixture_id = fixture.get("id")
        if not fixture_id:
            logger.warning("Skipping fixture due to missing ID.")
            return

        file_path = f"{JSON_DATA_DIR}/aucb_matches/{fixture_id}/fixture.json"
        try:
            write_file(file_path, fixture)
            logger.debug(f"Saved fixture {fixture_id}")
        except Exception as e:
            logger.error(f"Error saving fixture {fixture_id}: {e}")
    
    @staticmethod
    async def scrape_fixtures():
        """Main function to scrape all fixtures."""
        if MODE != 'prod':
            ensure_dir(f"{JSON_DATA_DIR}/aucb_matches")
            
        semaphore = asyncio.Semaphore(FIXTURES_CONCURRENCY_LIMIT)
        all_fixtures = []
        fetch_tasks = []

        logger.info("Starting fixtures scraping")
        async with AsyncSession(impersonate="chrome110", timeout=30) as session:
            for year in YEARS:
                task = asyncio.create_task(FixtureScraper.fetch_fixtures(session, year, semaphore))
                fetch_tasks.append(task)

            results = await asyncio.gather(*fetch_tasks)
            for yearly_fixtures in results:
                all_fixtures.extend(yearly_fixtures)

        logger.info(f"Fetched a total of {len(all_fixtures)} fixtures.")

        filtered_fixtures = [
            f for f in all_fixtures
            if f.get("gameTypeId") in VALID_GAME_TYPE_IDS
            and f.get("startDateTime", "")[:10] > PREV_DATE
        ]

        logger.info(f"Filtered down to {len(filtered_fixtures)} fixtures with gameTypeIds {VALID_GAME_TYPE_IDS}.")

        save_tasks = [asyncio.create_task(FixtureScraper.save_fixture(fixture)) for fixture in filtered_fixtures]
        await asyncio.gather(*save_tasks)

        logger.info(f"Finished processing and saving fixtures.")
        
        # Return fixture IDs that were saved
        return {f.get("id") for f in filtered_fixtures if f.get("id")}

class CricketScraper:
    def __init__(self):
        self.scorecard_urls = []
        self.comments_urls = []
        self.output_dir = JSON_DATA_DIR
        self.semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        self.current_proxy_index = 0
        self.fixture_ids = set()
        
    def get_next_proxy(self) -> Dict[str, str]:
        """Get the next proxy from the rotation"""
        proxy = PROXY_LIST[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(PROXY_LIST)
        return proxy
    
    def load_fixture_ids_from_directory(self) -> Set[int]:
        """Scan the json_data/aucb_matches directory to find all fixture IDs"""
        fixture_ids = set()
        
        if MODE == 'prod':
            try:
                # List all blobs with the prefix
                blobs = bucket.list_blobs(prefix=f"{JSON_DATA_DIR}/aucb_matches/")
                # Extract fixture IDs from paths
                for blob in blobs:
                    # Path format: json_data/aucb_matches/{fixture_id}/fixture.json
                    parts = blob.name.split('/')
                    if len(parts) >= 4 and parts[-1] == 'fixture.json':
                        try:
                            match_id = int(parts[-2])
                            fixture_ids.add(match_id)
                        except ValueError:
                            continue
            except Exception as e:
                logger.error(f"Error listing fixtures from GCP: {e}")
        else:
            base_dir = Path(f"{JSON_DATA_DIR}/aucb_matches")
            if not base_dir.exists():
                logger.warning(f"Output directory {base_dir} does not exist.")
                return fixture_ids
            
            for match_dir in base_dir.iterdir():
                if not match_dir.is_dir():
                    continue
                    
                try:
                    match_id = int(match_dir.name)
                    fixture_path = match_dir / "fixture.json"
                    
                    if fixture_path.exists():
                        fixture_ids.add(match_id)
                        logger.debug(f"Found existing fixture ID: {match_id}")
                except ValueError:
                    logger.warning(f"Invalid directory name (not an integer): {match_dir.name}")
                
        logger.info(f"Found {len(fixture_ids)} fixture IDs from directory structure")
        return fixture_ids
        
    def generate_urls(self) -> List[Dict[str, Any]]:
        """Generate URLs for both scorecard and comments based on fixture IDs"""
        urls = []
        
        for fixture_id in sorted(self.fixture_ids, reverse=True):
            base_path = f"{JSON_DATA_DIR}/aucb_matches/{fixture_id}"
            
            # Scorecard URL - check if file exists
            scorecard_file = f"{base_path}/scorecard.json"
            if not file_exists(scorecard_file):
                urls.append({
                    "url": BASE_SCORECARD_URL.format(fixture_id),
                    "fixture_id": fixture_id,
                    "type": "scorecard"
                })
            
            # Generate URLs for innings 1-4
            for inning_num in range(1, 5):
                inning_file = f"{base_path}/inning{inning_num}.json"
                if not file_exists(inning_file):
                    urls.append({
                        "url": BASE_COMMENTS_URL.format(fixture_id, inning_num),
                        "fixture_id": fixture_id,
                        "type": f"inning{inning_num}"
                    })
            if len(urls) > 1000:
                break

                    
        logger.info(f"Generated {len(urls)} URLs to scrape for {len(self.fixture_ids)} fixtures")
        return urls

    async def fetch_url(self, session: AsyncSession, url_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch data from a single URL using curl_cffi"""
        url = url_info["url"]
        fixture_id = url_info["fixture_id"]
        data_type = url_info["type"]
        
        async with self.semaphore:
            # Get a proxy from the rotation
            proxies = self.get_next_proxy()
            proxy_url = proxies['http']
            
            try:
                response = await session.get(
                    url,
                    headers=headers,
                    impersonate=random.choice(["chrome110", "chrome116", "chrome119", "chrome120", "chrome123", "chrome124", "chrome131"]),
                    proxies=proxies
                )

                if response.status_code != 200:
                    logger.warning(f"Failed to fetch {url} via proxy {proxy_url}, status: {response.status_code} - {response.reason}")
                    return None
                
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON from {url}. Status: {response.status_code}. Response text: {response.text[:200]}...")
                    return None

                # Skip if the response has only 1 key-value pair (indicates no data)
                if data and len(data) <= 1:
                    logger.debug(f"No useful data for fixture {fixture_id}, type {data_type}")
                    return None
                
                logger.info(f"Successfully fetched fixture {fixture_id}, type {data_type} via proxy {proxy_url}")    
                return {
                    "fixture_id": fixture_id,
                    "type": data_type,
                    "data": data
                }
            except Exception as e:
                logger.error(f"Error fetching {url} via proxy {proxy_url}: {e}")
                return None

    def is_valid_match(self, data: Dict[str, Any]) -> bool:
        """Check if a match meets our criteria"""
        try:
            # 1. Check if women's competition
            is_womens = data.get("fixture", {}).get("competition", {}).get("isWomensCompetition", True)
            if is_womens:
                return False
                
            # 2. Check if after 2019
            start_date_str = data.get("fixture", {}).get("startDateTime")
            if not start_date_str:
                return False
                
            start_date = datetime.datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
            if start_date.year < 2019:
                return False
                
            # 3. Check game type ID
            game_type_id = data.get("fixture", {}).get("gameTypeId")
            if game_type_id not in [1, 2, 3, 6, 24]:
                return False
                
            # 4. Check if no result
            result = data.get("fixture", {}).get("resultType", "No Result")
            if result == "No Result" or result == "Abandoned":
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error validating match: {e}")
            return False

    async def save_data(self, result: Dict[str, Any]) -> bool:
        """Save data to the appropriate JSON file"""
        if not result:
            return False
            
        fixture_id = result["fixture_id"]
        data_type = result["type"]
        data = result["data"]
        
        # For scorecard data, validate if it meets our criteria
        if data_type == "scorecard" and not self.is_valid_match(data):
            logger.debug(f"Match {fixture_id} does not meet criteria - skipping")
            return False
        
        # Determine the filename based on data type
        file_path = f"{JSON_DATA_DIR}/aucb_matches/{fixture_id}/{data_type}.json"
        
        # Save the data
        try:
            write_file(file_path, data)
            logger.info(f"Saved {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving {file_path}: {e}")
            return False

    async def process_batch(self, session: AsyncSession, batch: List[Dict[str, Any]]) -> None:
        """Process a batch of URLs using the provided session"""
        tasks = [self.fetch_url(session, url_info) for url_info in batch]
        results = await asyncio.gather(*tasks)
        
        # Save valid results
        save_tasks = [self.save_data(result) for result in results if result]
        await asyncio.gather(*save_tasks)

    async def scrape(self) -> None:
        """Main scraping function"""
        # Load fixture IDs from directory
        self.fixture_ids = self.load_fixture_ids_from_directory()
        
        if not self.fixture_ids:
            logger.error("No fixture IDs found. Make sure you've already run the previous script that fetches fixture data.")
            return
            
        # Generate URLs for all fixture IDs
        urls = self.generate_urls()
        
        # Create output directory if it doesn't exist
        if MODE != 'prod':
            ensure_dir(self.output_dir)
        
        # Process in batches
        batch_size = 1000
        
        async with AsyncSession(headers=headers, verify=True) as session:
            for i in range(0, len(urls), batch_size):
                batch = urls[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(urls)+batch_size-1)//batch_size}")
                await self.process_batch(session, batch)
                
                # Add a fixed delay between batches
                logger.info("Pausing for 5 seconds between batches")
                
        logger.info("BBB data scraping completed!")

async def main():
    """Main function that returns True on success, False on failure"""
    try:
        # First, scrape fixtures
        logger.info("Starting cricket data pipeline: fixtures scraping")
        fixture_ids = await FixtureScraper.scrape_fixtures()
        
        # Then scrape match data
        logger.info("Starting cricket data pipeline: BBB data scraping")
        scraper = CricketScraper()
        await scraper.scrape()
        
        logger.info("Complete cricket data pipeline finished successfully")
        return True  # Success
        
    except Exception as e:
        logger.error(f"Cricket data pipeline failed: {e}")
        return False  # Failure

if __name__ == "__main__":
    logger.info("Starting cricket data scraper")
    asyncio.run(main()) 