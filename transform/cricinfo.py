import orjson  # Faster JSON library
import math
import os
import glob
import logging
import time
import multiprocessing
from functools import partial
import re
import calendar
from datetime import datetime
import base64
import json
import fnmatch
from typing import List, Dict, Any, Optional, Tuple
from google.cloud import storage
from google.oauth2 import service_account

from dotenv import load_dotenv
load_dotenv()


import os
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
        print("✅ Google Cloud Logging integration enabled for Cricinfo Transformer")

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
    BBB_DATA_DIR = f'{OUTPUT_BASE_DIR}'
    

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
def read_file(file_path: str) -> dict:
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

def write_file(file_path: str, data: dict) -> None:
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

def list_files(directory: str, pattern: str = "*") -> List[str]:
    """List files from either local storage or GCP bucket"""
    if MODE == 'prod':
        try:
            blobs = bucket.list_blobs(prefix=directory)
            return [blob.name for blob in blobs if fnmatch.fnmatch(blob.name, pattern)]
        except Exception as e:
            logger.error(f"Error listing files from GCP: {directory} - {e}")
            raise
    else:
        return glob.glob(os.path.join(directory, pattern))

class CricinfoTransformer:
    # Team ID to name mapping for international teams
    TEAM_LOOKUP = {
        0: "Unknown",
        40: "Afghanistan",
        2: "Australia",
        25: "Bangladesh",
        1: "England",
        6: "India",
        29: "Ireland",
        5: "New Zealand",
        7: "Pakistan",
        3: "South Africa",
        8: "Sri Lanka",
        4: "West Indies",
        9: "Zimbabwe",
        28: "Namibia",
        33: "Nepal",
        15: "Netherlands",
        37: "Oman",
        20: "Papua New Guinea",
        30: "Scotland",
        27: "United Arab Emirates",
        11: "United States of America"
    }

    def validate_date(self, dob):
        """Validates and formats date components, handling invalid dates."""
        if not dob or not all(dob.get(k) is not None for k in ['date', 'month', 'year']):
            return None
            
        year = dob.get('year')
        month = dob.get('month')
        day = dob.get('date')
        
        # Check if month is valid
        if month < 1 or month > 12:
            return None
            
        # Get last day of month
        last_day = calendar.monthrange(year, month)[1]
        
        # Ensure day is valid
        if day < 1:
            day = 1
        elif day > last_day:
            day = last_day
            
        # Format as YYYY-MM-DD
        return f"{year:04d}-{month:02d}-{day:02d}"

    def get_bowling_kind(self, style):
        """Maps detailed bowling style to a general kind (pace/spin)."""
        if not style:
            return None
        style = style.lower()
        # Pace variations
        if ('fast' in style or
            'medium' in style or
            'pace' in style ):
            return "pace bowler"
        else:
            return "spin bowler"

    def determine_outcome(self, comment):
        """Determines the outcome string based on ball details."""
        if comment.get('isWicket'):
            return "wicket"
        if comment.get('wides', 0) > 0:
            return "wide"
        if comment.get('noballs', 0) > 0:
            # Check runs scored off the no-ball itself
            runs_off_bat = comment.get('batsmanRuns', 0)
            if runs_off_bat == 4:
                 return "noball+four" # Or just "noball"? Example doesn't cover this well. Let's stick to simpler outcomes.
            if runs_off_bat == 6:
                 return "noball+six"
            if runs_off_bat > 0:
                 return "noball+run" # Assume any runs > 0 are 'run'
            return "noball" # No runs off bat, just the noball extra
        if comment.get('isFour'):
            return "four"
        if comment.get('isSix'):
            return "six"
        if comment.get('batsmanRuns', 0) > 0 or comment.get('legbyes', 0) > 0 or comment.get('byes', 0) > 0:
             # If total runs > 0 and it's not a boundary/extra classified above, call it 'run'
             # Need to be careful if byes/legbyes can be 4s. Assuming simple 'run' if not four/six.
             # The example 'run' has score 2, so let's classify any non-zero score (excluding wides/noballs already handled) that isn't a 4 or 6 as 'run'.
             if comment.get('totalRuns', 0) > 0:
                 return "run"
             else: # Should not happen if batsmanRuns > 0 etc, but as fallback
                 return "no run"

        return "no run"

    def format_ball_id(self, overs_actual):
        """Formats overs_actual (e.g., 18.6) into ball_id (e.g., '18.06')."""
        if overs_actual is None:
            return None
        over_part = int(overs_actual)
        # Ball part needs care: 18.1 -> ball 1, 18.6 -> ball 6
        # Calculate ball number based on fractional part
        ball_part_decimal = round((overs_actual - over_part) * 10) # Get 1, 2, 3, 4, 5, 6
        if ball_part_decimal == 0 and overs_actual > 0: # End of a previous over reported as .0
             # This case is tricky, might indicate end of over summary rather than a ball.
             # Or if it's 0.0, it's before first ball.
             # Let's assume overs_actual like 18.6 means ball 6 of over 19 (over number is 1-based)
             # And overs_actual 19.0 means end of over 19.
             # Let's re-evaluate based on overNumber and ballNumber from comment if available
             return f"{over_part}.0{ball_part_decimal}" # Standard formatting like example
        elif ball_part_decimal > 0:
             return f"{over_part}.0{ball_part_decimal}"
        else: # Handle 0.0 case or potential errors
             return f"{over_part}.00"


    def create_bbb_json(self, commentary_data):
        """
        Transforms commentary JSON into the desired ball-by-ball format.

        Args:
            commentary_data (dict): The loaded JSON data from the commentary file.

        Returns:
            list: A list of dictionaries, each representing a ball bowled.
        """
        match_info = commentary_data.get('match', {})
        content = commentary_data.get('content', {})
        comments = content.get('comments', [])
        innings_data = content.get('innings', [])
        # print(f"comments: {len(comments)}") # Keep for debugging if needed
        # print(f"innings_data: {len(innings_data)}")

        # Sort comments chronologically by inning, over, and ball number
        comments.sort(key=lambda c: (c.get('inningNumber', 0), c.get('overNumber', 0), c.get('ballNumber', 0)))

        # --- Pre-process Static Data ---
        p_match = str(match_info.get('objectId')) # Use objectId as p_match based on input structure
        date = match_info.get('startDate', '').split('T')[0] if match_info.get('startDate') else None
        year = date.split('-')[0] if date else None
        ground = match_info.get('ground', {}).get('name')
        country = match_info.get('ground', {}).get('country', {}).get('name')
        winner_id = match_info.get('winnerTeamId')
        toss_winner_id = match_info.get('tossWinnerTeamId')
        competition = match_info.get('internationalClassId') # T20 -> T20I? Example shows T20I
        if competition:
            competition = 'T20I' 
        else:
            competition = match_info.get('series', {}).get('longName') # Match example
        max_balls = match_info.get('scheduledOvers', 20) * 6 if match_info.get('scheduledOvers') else 120
        

        # --- Build Player/Team Lookups ---
        player_details = {} # id -> {name, team_id, team_name, bat_hand, bowl_style, bowl_kind, final_out_status}
        team_names = {} # id -> name
        team_ids = {} # name -> id

        for team_info in match_info.get('teams', []):
            team_id = team_info.get('team', {}).get('id')
            team_name = team_info.get('team', {}).get('longName')
            if team_id and team_name:
                team_names[team_id] = team_name
                team_ids[team_name] = team_id
                # Add captain details if not already present
                captain = team_info.get('captain')
                if captain and captain.get('id') not in player_details:
                     player_details[captain['id']] = {
                         'name': captain.get('longName'),
                         'team_id': team_id,
                         'team_name': team_name,
                         'bat_hand': next(iter(captain.get('longBattingStyles', [])), None),
                         'bowl_style': next(iter(captain.get('longBowlingStyles', [])), None),
                         'bowl_kind': self.get_bowling_kind(next(iter(captain.get('longBowlingStyles', [])), None)),
                         'date_of_birth': self.validate_date(captain.get('dateOfBirth')),
                         'country': self.TEAM_LOOKUP.get(captain.get('countryTeamId', 0)),
                         'final_out_status': 'unknown' # Will be updated from innings data
                     }

        winner_name = team_names.get(winner_id)
        toss_winner_name = team_names.get(toss_winner_id)

        # Populate/Update player details from innings data
        for inn_idx, inn in enumerate(innings_data):
            team_id = inn.get('team', {}).get('id')
            team_name = team_names.get(team_id)

            # Batsmen
            for batsman in inn.get('inningBatsmen', []):
                player_id = batsman.get('player', {}).get('id')
                if not player_id: continue

                bat_style = next(iter(batsman.get('player', {}).get('longBattingStyles', [])), None)
                bowl_style = next(iter(batsman.get('player', {}).get('longBowlingStyles', [])), None)
                bowl_kind = self.get_bowling_kind(bowl_style)

                if player_id not in player_details:
                     player_details[player_id] = {
                         'name': batsman.get('player', {}).get('longName'),
                         'team_id': team_id,
                         'team_name': team_name,
                         'bat_hand': bat_style,
                         'bowl_style': bowl_style,  
                         'bowl_kind': bowl_kind,
                         'date_of_birth': self.validate_date(batsman.get('player',{}).get('dateOfBirth')),
                         'country': self.TEAM_LOOKUP.get(batsman.get('player',{}).get('countryTeamId', 0)),
                         'final_out_status': str(batsman.get('isOut', False)).lower()
                     }
                else: # Update existing entry if needed (e.g., captain might bat)
                     player_details[player_id]['bat_hand'] = bat_style
                     player_details[player_id]['final_out_status'] = str(batsman.get('isOut', False)).lower()
                     if player_details[player_id]['team_id'] is None: # If captain wasn't assigned team yet
                          player_details[player_id]['team_id'] = team_id
                          player_details[player_id]['team_name'] = team_name


            # Bowlers
            for bowler in inn.get('inningBowlers', []):
                player_id = bowler.get('player', {}).get('objectId')
                if not player_id: continue

                bat_style = next(iter(bowler.get('player', {}).get('longBattingStyles', [])), None)
                bowl_style = next(iter(bowler.get('player', {}).get('longBowlingStyles', [])), None)
                bowl_kind = self.get_bowling_kind(bowl_style)

                if player_id not in player_details:
                     # Find the team ID for this bowler (might be complex if not captain/batsman)
                     # Assume they belong to the *other* team for this inning
                     opp_team_id = None
                     opp_team_name = None
                     for t_id, t_name in team_names.items():
                         if t_id != team_id:
                             opp_team_id = t_id
                             opp_team_name = t_name
                             break

                     player_details[player_id] = {
                         'name': bowler.get('player', {}).get('longName'),
                         'team_id': opp_team_id,
                         'team_name': opp_team_name,
                         'bat_hand': bat_style,
                         'bowl_style': bowl_style,
                         'bowl_kind': bowl_kind,
                         'date_of_birth': self.validate_date(bowler.get('player',{}).get('dateOfBirth')),
                         'country': self.TEAM_LOOKUP.get(bowler.get('player',{}).get('countryTeamId', 0)),
                         'final_out_status': 'unknown' # Will be updated if they bat
                     }
                else: # Update existing entry
                     player_details[player_id]['bowl_style'] = bowl_style
                     player_details[player_id]['bowl_kind'] = bowl_kind
                     # Update team info if missing (e.g. captain who bowls)
                     if player_details[player_id]['team_id'] is None:
                          for t_id, t_name in team_names.items():
                             if t_id != team_id:
                                 player_details[player_id]['team_id'] = t_id
                                 player_details[player_id]['team_name'] = t_name
                                 break


        # --- Process Ball-by-Ball Data ---
        bbb_output = []
        # Cumulative stats tracking (per player and per inning)
        batsman_stats = {} # key: (inning, player_id), value: {'runs': r, 'bf': b}
        bowler_stats = {}  # key: (inning, player_id), value: {'balls': b, 'runs': r, 'wkts': w}
        inning_stats = {}  # key: inning, value: {'runs': r, 'wkts': w, 'balls': b}

        # --- Determine Target ---
        target = None
        if len(innings_data) > 0 and innings_data[0].get('runs') is not None:
            target = innings_data[0]['runs'] + 1

        for comment in comments:
            # Check if it's a valid ball comment (has essential fields)
            if not all(k in comment for k in ['inningNumber', 'overNumber', 'ballNumber', 'batsmanPlayerId', 'bowlerPlayerId', 'oversActual']):
                continue

            # Extract basic info
            inning_num = comment['inningNumber']
            # max_balls=comment['ballLimit']

            batsman_id = comment['batsmanPlayerId']
            bowler_id = comment['bowlerPlayerId']

            # Skips comments that don't have player info (might be over summaries etc)
            if not batsman_id or not bowler_id:
                 #print(f"Warning: No batsman or bowler ID for ball {comment.get('oversActual')}. Skipping.")
                 continue

            # Get player details safely
            batsman_detail = player_details.get(batsman_id, {})
            bowler_detail = player_details.get(bowler_id, {})

            bat_name = batsman_detail.get('name')
            bowl_name = bowler_detail.get('name')
            bat_team_id = batsman_detail.get('team_id')
            bowl_team_id = bowler_detail.get('team_id')

            # Ensure we have team info for both players
            if not bat_team_id or not bowl_team_id:
                # Try inferring from inning number if missing (Simple logic based on standard match structure)
                team_ids_in_match = list(team_names.keys())
                if len(team_ids_in_match) == 2:
                    inn1_bat_team_id = team_ids_in_match[0] # Assuming team 0 bats first
                    inn1_bowl_team_id = team_ids_in_match[1]
                    if inning_num == 1:
                        if not bat_team_id: bat_team_id = inn1_bat_team_id
                        if not bowl_team_id: bowl_team_id = inn1_bowl_team_id
                    elif inning_num == 2:
                        if not bat_team_id: bat_team_id = inn1_bowl_team_id # Team 1 bowls in inn 2
                        if not bowl_team_id: bowl_team_id = inn1_bat_team_id # Team 0 bats in inn 2

                # If still missing after inference, log and skip
                if not bat_team_id or not bowl_team_id:
                     #print(f"Warning: Could not determine teams for ball {comment.get('oversActual')}. Skipping.")
                     continue # Skip if team info is crucial and missing


            bat_team_name = team_names.get(bat_team_id)
            bowl_team_name = team_names.get(bowl_team_id)

            # Initialize stats if first time seeing player/inning
            if inning_num not in inning_stats:
                inning_stats[inning_num] = {'runs': 0, 'wkts': 0, 'balls': 0}
            if (inning_num, batsman_id) not in batsman_stats:
                batsman_stats[(inning_num, batsman_id)] = {'runs': 0, 'bf': 0}
            if (inning_num, bowler_id) not in bowler_stats:
                bowler_stats[(inning_num, bowler_id)] = {'balls': 0, 'runs': 0, 'wkts': 0}

            # --- Update Stats based on current ball ---
            current_inning_stat = inning_stats[inning_num]
            current_batsman_stat = batsman_stats[(inning_num, batsman_id)]
            current_bowler_stat = bowler_stats[(inning_num, bowler_id)]

            total_runs_this_ball = comment.get('totalRuns', 0)
            batsman_runs_this_ball = comment.get('batsmanRuns', 0)
            legbyes_this_ball = comment.get('legbyes', 0)
            byes_this_ball = comment.get('byes', 0)
            wides_this_ball = comment.get('wides', 0)
            noballs_this_ball = comment.get('noballs', 0)
            is_wicket = comment.get('isWicket', False)

            # Innings score update
            current_inning_stat['runs'] += total_runs_this_ball

            # Balls count update (only for legal deliveries)
            is_legal_delivery = (wides_this_ball == 0 and noballs_this_ball == 0)
            if is_legal_delivery:
                 current_inning_stat['balls'] += 1
                 current_bowler_stat['balls'] += 1
                 current_batsman_stat['bf'] += 1

            # Batsman runs update
            current_batsman_stat['runs'] += batsman_runs_this_ball

            # Bowler runs conceded update (all runs except byes/legbyes)
            runs_conceded = total_runs_this_ball - byes_this_ball - legbyes_this_ball
            current_bowler_stat['runs'] += runs_conceded

            # Wicket update
            if is_wicket:
                # Ensure wicket wasn't run out (bowler doesn't always get credit)
                dismissal_type = comment.get('dismissalType')
                # Standard dismissal types credited to bowler: 1 (caught), 2 (bowled), 3 (lbw), 5 (stumped), 11 (hit wicket)
                if dismissal_type in [1, 2, 3, 5, 11]:
                    current_bowler_stat['wkts'] += 1
                current_inning_stat['wkts'] += 1

            # --- Prepare Output Fields ---
            ball_id_str = self.format_ball_id(comment.get('oversActual'))
            if not ball_id_str: # Skip if we can't format ball id reliably
                 continue

            outcome = self.determine_outcome(comment)
            score = total_runs_this_ball # Total runs for the ball event

            # Bowler's overs calculation
            bowler_balls_completed = current_bowler_stat['balls']
            bowler_overs_part = bowler_balls_completed // 6
            bowler_balls_part = bowler_balls_completed % 6
            cur_bowl_ovr = bowler_overs_part + (bowler_balls_part / 10)

            # Calculate remaining runs/balls/RRR (only for inning 2)
            inns_runs_rem = None
            inns_balls_rem = None
            inns_rrr = None
            if inning_num == 2 and target is not None:
                inns_runs_rem = float(target - current_inning_stat['runs'])
                inns_balls_rem = max_balls - current_inning_stat['balls']
                if inns_balls_rem > 0:
                    inns_rrr = round((inns_runs_rem / inns_balls_rem) * 6, 2) if inns_runs_rem > 0 else 0.0
                elif inns_runs_rem <= 0: # Target reached or passed
                     inns_rrr = 0.0
                else: # Balls finished, target not reached
                     inns_rrr = None # Represent infinity/undefined as None

            # Calculate current run rate
            inns_rr = None
            if current_inning_stat['balls'] > 0:
                inns_rr = round((current_inning_stat['runs'] * 6) / current_inning_stat['balls'], 2)

            # Ball faced count for this specific ball
            ballfaced = 1 if is_legal_delivery else 0 # Batsman faces only legal deliveries

            # Runs scored by batsman off this ball
            batruns = batsman_runs_this_ball

            # Runs conceded by bowler off this ball (excluding byes/legbyes)
            bowlruns = runs_conceded

            # Dismissal text
            dismissal_text = None
            if is_wicket and comment.get('dismissalText'):
                 # Use the 'short' version if available, else 'long'
                 dismissal_text = comment['dismissalText'].get('short') or comment['dismissalText'].get('long')

            # Map shot control
            control_map = {1: 1.0, 2: 0.0} # Assuming 1=good, 2=poor control based on example
            control_val = control_map.get(comment.get('shotControl'), None)


            ball_data = {
              "p_match": p_match,
              "inns": inning_num,
              "team1":match_info.get('teams')[0]['team'].get('longName'),
              "team2":match_info.get('teams')[1]['team'].get('longName'),



              "p_bat":  batsman_id,
              "bat": bat_name,
              'bat_country': batsman_detail.get('country'),
              "bat_date_of_birth": batsman_detail.get('date_of_birth'),

              "p_non_striker": comment.get('nonStrikerPlayerId'), # Player ID of non-striker
              "non_striker": player_details.get(comment.get('nonStrikerPlayerId'), {}).get('name'),
              "non_striker_date_of_birth": player_details.get(comment.get('nonStrikerPlayerId'), {}).get('date_of_birth'),
              "non_striker_country": player_details.get(comment.get('nonStrikerPlayerId'), {}).get('country'),

              "team_bat": bat_team_name,
              "team_bowl": bowl_team_name,

              "p_bowl": bowler_id,
              "bowl": bowl_name,
              "bowl_date_of_birth": bowler_detail.get('date_of_birth'),
              'bowl_country': bowler_detail.get('country'),

              "ball": comment.get('ballNumber'), # Ball number within the over (1-6)
              "ball_id": comment.get('oversUnique'), # e.g., "5.04"
              "outcome": outcome,
              "score": score, # Total score from the ball event
              "out": is_wicket,
              "dismissal": dismissal_text,
              "p_out": comment.get('outPlayerId'),
              "over": comment.get('overNumber'), # Over number (1-based)
              "noball": noballs_this_ball,
              "wide": wides_this_ball, # Wide count
              "byes": byes_this_ball,
              "legbyes": legbyes_this_ball,

              "cur_bat_runs": current_batsman_stat['runs'],
              "cur_bat_bf": current_batsman_stat['bf'],
              "cur_bowl_ovr": cur_bowl_ovr, # e.g., "0.4"
              "cur_bowl_wkts": current_bowler_stat['wkts'],
              "cur_bowl_runs": current_bowler_stat['runs'],
              "inns_runs": current_inning_stat['runs'],
              "inns_wkts": current_inning_stat['wkts'],
              "inns_balls": current_inning_stat['balls'],
              "inns_runs_rem": inns_runs_rem if inns_runs_rem is not None else None,
              "inns_balls_rem": inns_balls_rem if inns_balls_rem is not None else None,
              "inns_rr": inns_rr if inns_rr is not None else None,
              "inns_rrr": inns_rrr if inns_rrr is not None else None,
              "target": float(target) if target is not None else None,
              "max_balls": max_balls,

              "date": date,
              "year": year,
              "ground": ground,
              "country": country,
              "winner": winner_name,
              "toss": toss_winner_name,
              "toss_decision": "bat" if match_info.get('tossWinnerChoice') == 1 else "bowl",
              "win_type": "wickets" if "wickets" in match_info.get('statusText') else "runs",
              "win_margin": int(re.search(r'by (\d+) (runs|wickets)', match_info.get('statusText', '')).group(1)) if re.search(r'by (\d+) (runs|wickets)', match_info.get('statusText', '')) else None,
              "competition": competition,
              "bat_hand": batsman_detail.get('bat_hand'),
              "bowl_style": bowler_detail.get('bowl_style'),
              "bowl_kind": bowler_detail.get('bowl_kind'),

              "batruns": batruns, # Runs scored *by batsman* off this ball
              "ballfaced": ballfaced, # Did batsman face this ball (0 for wide/noball)
              "bowlruns": bowlruns, # Runs conceded *by bowler* off this ball (no byes/legbyes)
            #   "bat_out": batsman_detail.get('final_out_status', 'false'), # Final status in the match
              "wagonX": comment.get('wagonX', 0), # Default to 0 if missing
              "wagonY": comment.get('wagonY', 0),
              "wagonZone": comment.get('wagonZone', 0),
              "line": comment.get('pitchLine'),
              "length": comment.get('pitchLength'),
              "shot": comment.get('shotType'),
              "control": control_val if control_val is not None else None,
              "predscore": -1 if comment.get('predictions') is None else comment.get('predictions').get('score', -1),
              "wprob": -1.0 if comment.get('predictions') is None else comment.get('predictions').get('winProbability', -1.0)
            }

            bbb_output.append(ball_data)

        return bbb_output

# --- Worker Function for Parallel Processing ---
def process_single_file(transformer, output_dir, file_path):
    """
    Reads a single JSON file, transforms it using the transformer,
    and saves the output to the specified directory.
    Returns a tuple (bool: success, str: file_path, str: error_message | None).
    """
    # Get the original base filename and change extension to .ndjson
    base_filename = os.path.basename(file_path)
    base_filename = base_filename.replace('.json', '.ndjson')
    # Construct the full output path
    if MODE == 'prod':
        output_path = f"cricinfo/{base_filename}"
    else:
        output_path = f"{BBB_DATA_DIR}/cricinfo/{base_filename}"
    
    # Skip processing if output file already exists
    if file_exists(output_path):
        logger.info(f"Skipped - file already processed: {output_path}")
        return True, file_path, "Skipped - file already processed"
        
    try:
        # Read and transform the data
        input_json_data = read_file(file_path)
        output_bbb_data = transformer.create_bbb_json(input_json_data)

        # Save the transformed data
        write_file(output_path, output_bbb_data)
        return True, file_path, None

    except FileNotFoundError:
        return False, file_path, f"Error: File not found at {file_path}"
    except orjson.JSONDecodeError:
        return False, file_path, f"Error: Could not decode JSON from {file_path}"
    except Exception as e:
        # Capture the exception details for better debugging
        import traceback
        error_msg = f"An unexpected error occurred processing {file_path}: {e}\n{traceback.format_exc()}"
        return False, file_path, error_msg

# --- Main Execution Logic ---
def main():
    """Main function that returns True on success, False on failure"""
    # Add file handler for detailed logging

    
    input_dir = f'{JSON_DATA_DIR}/cricinfo_matches/commentary'
    output_dir = f'{BBB_DATA_DIR}/cricinfo'

    # Create output directory if it doesn't exist (local mode only)
    if MODE != 'prod':
        ensure_dir(output_dir)

    # Find all JSON files in the input directory
    json_files = list_files(input_dir, "*.json")

    total_files = len(json_files)
    logger.info(f"Found {total_files} JSON files to process in {input_dir}.")

    if not json_files:
        logger.info("No JSON files found to process. Exiting.")
        return False

    transformer = CricinfoTransformer() # Instantiate transformer once

    # Determine number of worker processes
    num_workers = max(1, os.cpu_count() - 1 if os.cpu_count() else 1) # Leave one core free
    logger.info(f"Using {num_workers} worker processes.")

    # Performance tracking variables
    start_time = time.time()
    batch_start_time = start_time
    files_processed_count = 0
    files_succeeded_count = 0
    files_failed_count = 0
    batch_size = 1000

    # Create a partial function with the fixed transformer and output_dir arguments
    process_func = partial(process_single_file, transformer, output_dir)

    # Use multiprocessing Pool
    logger.info("Starting processing pool...")
    with multiprocessing.Pool(processes=num_workers) as pool:
        # Use imap_unordered for potentially better performance as results are yielded as they complete
        results = pool.imap_unordered(process_func, json_files)

        for success, file_path, error_message in results:
            files_processed_count += 1
            if success:
                files_succeeded_count += 1
            else:
                files_failed_count += 1
                logger.error(f"Failed processing {file_path}: {error_message}")

            # Log progress every batch_size files or at the end
            if files_processed_count % batch_size == 0 or files_processed_count == total_files:
                current_time = time.time()
                batch_time = current_time - batch_start_time
                total_time_so_far = current_time - start_time
                logger.info(
                    f"Processed {files_processed_count}/{total_files} files. "
                    f"Current batch ({min(batch_size, files_processed_count % batch_size or batch_size)} files) took {batch_time:.2f} seconds. "
                    f"Total time: {total_time_so_far:.2f} seconds. "
                    f"Success: {files_succeeded_count}, Failed: {files_failed_count}"
                )
                batch_start_time = current_time # Reset batch timer

    # Final statistics
    total_time = time.time() - start_time
    logger.info(f"Finished processing all {total_files} files.")
    logger.info(f"Total time: {total_time:.2f} seconds.")
    if files_succeeded_count > 0:
         logger.info(f"Average time per successfully processed file: {total_time/files_succeeded_count:.3f} seconds.")
    logger.info(f"Successful: {files_succeeded_count}, Failed: {files_failed_count}")

    
    # Return True if any files were processed successfully
    return files_succeeded_count > 0

if __name__ == "__main__":
    main()