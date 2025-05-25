#!/usr/bin/env python
import os
import orjson  # Faster JSON library
import glob
import argparse
from pathlib import Path
import logging
import datetime
import multiprocessing
from functools import partial
import time
import base64
import json
from google.cloud import storage
from google.oauth2 import service_account
import fnmatch
import os
from dotenv import load_dotenv
load_dotenv()
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
        print("✅ Google Cloud Logging integration enabled for AUCB Transformer")

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

# Set up environment-based paths

if MODE == 'prod':
    # GCP Storage paths
    OUTPUT_BASE_DIR = 'cricket-data-1'
    JSON_DATA_DIR = 'json_data'
    


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
    BBB_DATA_DIR = 'bbb_data'

# Helper functions for file operations
def read_file(file_path):
    """Read a file from either local storage or GCP bucket"""
    if MODE == 'prod':
        try:
            blob = bucket.blob(file_path)
            content = blob.download_as_bytes()
            return orjson.loads(content)
        except Exception as e:
            logger.error(f"Error reading from GCP: {file_path} - {e}")
            raise
    else:
        with open(file_path, 'rb') as f:
            return orjson.loads(f.read())

def write_file(file_path, data):
    """Write data to either local storage or GCP bucket in NDJSON format"""
    if MODE == 'prod':
        try:
            blob = bucket.blob(file_path)
            # Convert list of records to NDJSON format
            if isinstance(data, list):
                ndjson_content = '\n'.join(orjson.dumps(record).decode('utf-8') for record in data)
            else:
                ndjson_content = orjson.dumps(data).decode('utf-8')
            blob.upload_from_string(ndjson_content)
        except Exception as e:
            logger.error(f"Error writing to GCP: {file_path} - {e}")
            raise
    else:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            # Convert list of records to NDJSON format
            if isinstance(data, list):
                for record in data:
                    f.write(orjson.dumps(record).decode('utf-8') + '\n')
            else:
                f.write(orjson.dumps(data).decode('utf-8') + '\n')

def ensure_dir(dir_path):
    """Ensure directory exists (only needed for local storage)"""
    if MODE != 'prod':
        os.makedirs(dir_path, exist_ok=True)

def list_files(directory, pattern="*"):
    """List files from either local storage or GCP bucket"""
    if MODE == 'prod':
        try:
            blobs = bucket.list_blobs(prefix=directory)
            return [blob.name for blob in blobs if fnmatch.fnmatch(blob.name, pattern)]
        except Exception as e:
            logger.error(f"Error listing files from GCP: {directory} - {e}")
            raise
    else:
        # print(glob.glob(os.path.join(directory, pattern)))
        return glob.glob(os.path.join(directory, pattern))

def file_exists(file_path):
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

def create_player_lookups(scorecard_data):
    """Create dictionaries for fast player data lookup."""
    players = scorecard_data.get("players", [])
    
    name_lookup = {}
    dob_lookup = {}
    country_lookup = {}
    
    for player in players:
        player_id = player.get("id")
        if player_id:
            name_lookup[player_id] = player.get("displayName", "")
            
            # Process DOB
            dob = player.get("dob", None)
            if dob:
                try:
                    date_obj = datetime.datetime.strptime(dob, "%Y-%m-%dT%H:%M:%SZ")
                    dob_lookup[player_id] = date_obj.strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    dob_lookup[player_id] = dob
            else:
                dob_lookup[player_id] = None
                
            country_lookup[player_id] = player.get("nationality", None)
    
    return name_lookup, dob_lookup, country_lookup

def extract_match_data(fixture_data, scorecard_data):
    """Extract match-level data that doesn't change per ball."""
    home_team = fixture_data.get("homeTeam", {}).get("name", "")
    away_team = fixture_data.get("awayTeam", {}).get("name", "")
    
    toss_winner = home_team if fixture_data.get("homeTeam", {}).get("isTossWinner") else away_team
    match_winner = home_team if fixture_data.get("homeTeam", {}).get("isMatchWinner") else away_team
    
    ground = fixture_data.get("venue", {}).get("name", "")
    match_date_raw = fixture_data.get("startDateTime", "")
    
    # Format date as YYYY-MM-DD
    match_date = ""
    if match_date_raw:
        try:
            date_obj = datetime.datetime.strptime(match_date_raw, "%Y-%m-%dT%H:%M:%SZ")
            match_date = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            match_date = match_date_raw
    
    competition = fixture_data.get("competition", {}).get("name", "")
    
    return {
        "fixtureId": fixture_data.get("id"),
        "team1": home_team,
        "team2": away_team,
        "matchDate": match_date,
        "format": fixture_data.get("gameType"),
        "ground": ground,
        "competition": competition,
        "tossWinner": toss_winner,
        "tossDecision": fixture_data.get("tossDecision"),
        "matchWinner": match_winner,
        "winType": scorecard_data.get("fixture", {}).get("winType"),
        "winMargin": scorecard_data.get("fixture", {}).get("winningMargin"),
        "home_team": home_team,
        "away_team": away_team
    }

def process_match(match_id):
    """Process a single match and create ball-by-ball data."""
    # Define file paths
    base_path = f"{JSON_DATA_DIR}/aucb_matches/{match_id}"
    fixture_path = f"{base_path}/fixture.json"
    
    # Read fixture data
    try:
        fixture_data = read_file(fixture_path)
    except Exception as e:
        return False, match_id, f"Error reading fixture data: {e}"
        
    if fixture_data.get("resultType") == "No Result" or fixture_data.get("resultType") == "Abandoned":
        return True, match_id, "Skipped - no result or abandoned"

    # Define other file paths
    inning1_path = f"{base_path}/inning1.json"
    inning2_path = f"{base_path}/inning2.json"
    innings3_path = f"{base_path}/inning3.json"
    innings4_path = f"{base_path}/inning4.json"
    scorecard_path = f"{base_path}/scorecard.json"
    
    # Check if output file already exists
    if MODE == 'prod':
        output_path = f"aucb/{match_id}_commentary.ndjson"
    else:
        output_path = f"{BBB_DATA_DIR}/aucb/{match_id}_commentary.ndjson"
    
    if file_exists(output_path):
        return True, match_id, "Skipped - file already processed"
    
    # Check if all required files exist
    required_files = [fixture_path, inning1_path, inning2_path, scorecard_path]
    for file_path in required_files:
        if not file_exists(file_path):
            return False, match_id, f"Missing required file: {file_path}"
    
    try:
        # Load JSON data
        scorecard_data = read_file(scorecard_path)
        
        # Determine innings based on game type
        if fixture_data.get("gameType") == "Test":
            innings = [1, 2, 3, 4]
        else:
            innings = [1, 2]
        
        # Create player lookup dictionaries for O(1) access
        name_lookup, dob_lookup, country_lookup = create_player_lookups(scorecard_data)
        
        # Extract match-level data once
        match_data = extract_match_data(fixture_data, scorecard_data)
        
        # Process innings data
        all_balls = []
        
        for inning_num in innings:
            inning_path = f"{base_path}/inning{inning_num}.json"
            
            # Skip if inning file doesn't exist (for Test matches)
            if not file_exists(inning_path):
                continue
                
            inning_data = read_file(inning_path)
            
            # Determine batting and bowling teams for this inning
            if inning_num in [1, 3]:
                batting_team = match_data["home_team"]
                bowling_team = match_data["away_team"]
            else:
                batting_team = match_data["away_team"]
                bowling_team = match_data["home_team"]
            
            # Process each over in the innings
            for over in inning_data.get("inning", {}).get("overs", []):
                over_number = over.get("overNumber")
                
                # Process each ball in the over
                for ball in over.get("balls", []):
                    # Get player IDs
                    batsman_id = ball.get("battingPlayerId")
                    bowler_id = ball.get("bowlerPlayerId")
                    non_striker_id = ball.get("nonStrikeBattingPlayerId")
                    dismissed_id = ball.get("dismissalPlayerId")
                    
                    # Create ball data using pre-computed match data and fast lookups
                    ball_data = {
                        **match_data,  # Spread match-level data
                        "inningNumber": inning_num,
                        "battingTeam": batting_team,
                        "bowlingTeam": bowling_team,
                        "battingPlayer": name_lookup.get(batsman_id),
                        "battingPlayerDob": dob_lookup.get(batsman_id),
                        "battingPlayerCountry": country_lookup.get(batsman_id),
                        "nonStrikerPlayer": name_lookup.get(non_striker_id),
                        "nonStrikerPlayerDob": dob_lookup.get(non_striker_id),
                        "nonStrikerPlayerCountry": country_lookup.get(non_striker_id),
                        "bowlingPlayer": name_lookup.get(bowler_id),
                        "bowlingPlayerDob": dob_lookup.get(bowler_id),
                        "bowlingPlayerCountry": country_lookup.get(bowler_id),
                        "dismissedPlayer": name_lookup.get(dismissed_id),
                        "overNumber": over_number,
                    }
                    
                    # Add all keys from ball except 'comments'
                    for key, value in ball.items():
                        if key != 'comments':
                            ball_data[key] = value
                    
                    all_balls.append(ball_data)
        
        # Ensure output directory exists (only for local storage)
        if MODE != 'prod':
            ensure_dir(os.path.dirname(output_path))
        
        # Save the processed data
        write_file(output_path, all_balls)
        
        return True, match_id, None
    
    except Exception as e:
        import traceback
        error_msg = f"Error processing match {match_id}: {e}\n{traceback.format_exc()}"
        return False, match_id, error_msg

def main():
    """Process all match directories using multiprocessing."""

    
    # Use the existing list_files utility to find fixture files
    fixture_files = list_files(f"{JSON_DATA_DIR}/aucb_matches", "*/fixture.json")
    
    # Extract match IDs from the fixture file paths
    match_ids = []
    for file_path in fixture_files:
        # Normalize path separators for cross-platform compatibility
        
        normalized_path = file_path.replace('\\', '/')  # Convert backslashes to forward slashes
        parts = normalized_path.split('/')
        if len(parts) >= 3 and parts[-1] == 'fixture.json':
            match_id = parts[-2]
            match_ids.append(match_id)
    
    total_matches = len(match_ids)
    logger.info(f"Found {total_matches} match directories")
    
    if not match_ids:
        logger.warning("No match directories found")
        return False
    
    # Determine number of worker processes
    num_workers = max(1, multiprocessing.cpu_count() - 1 if multiprocessing.cpu_count() else 1)
    logger.info(f"Using {num_workers} worker processes")
    
    # Performance tracking variables
    start_time = time.time()
    batch_start_time = start_time
    matches_processed_count = 0
    matches_succeeded_count = 0
    matches_failed_count = 0
    matches_skipped_count = 0
    batch_size = 100  # Smaller batch size for match processing
    
    # Use multiprocessing Pool
    logger.info("Starting processing pool...")
    with multiprocessing.Pool(processes=num_workers) as pool:
        # Use imap_unordered for better performance as results are yielded as they complete
        results = pool.imap_unordered(process_match, match_ids)
        
        for success, match_id, error_message in results:
            matches_processed_count += 1
            if success:
                if error_message and "Skipped" in error_message:
                    matches_skipped_count += 1
                else:
                    matches_succeeded_count += 1
            else:
                matches_failed_count += 1
                logger.error(f"Failed processing match {match_id}: {error_message}")
            
            # Log progress every batch_size matches or at the end
            if matches_processed_count % batch_size == 0 or matches_processed_count == total_matches:
                current_time = time.time()
                batch_time = current_time - batch_start_time
                total_time_so_far = current_time - start_time
                logger.info(
                    f"Processed {matches_processed_count}/{total_matches} matches. "
                    f"Current batch ({min(batch_size, matches_processed_count % batch_size or batch_size)} matches) took {batch_time:.2f} seconds. "
                    f"Total time: {total_time_so_far:.2f} seconds. "
                    f"Success: {matches_succeeded_count}, Skipped: {matches_skipped_count}, Failed: {matches_failed_count}"
                )
                batch_start_time = current_time  # Reset batch timer
    
    # Final statistics
    total_time = time.time() - start_time
    logger.info(f"Finished processing all {total_matches} matches.")
    logger.info(f"Total time: {total_time:.2f} seconds.")
    if matches_succeeded_count > 0:
        logger.info(f"Average time per successfully processed match: {total_time/matches_succeeded_count:.3f} seconds.")
    logger.info(f"Successful: {matches_succeeded_count}, Skipped: {matches_skipped_count}, Failed: {matches_failed_count}")
    
    # Return True if any matches were processed successfully, False if all failed
    return matches_succeeded_count > 0 or matches_skipped_count > 0

if __name__ == "__main__":
    main()
