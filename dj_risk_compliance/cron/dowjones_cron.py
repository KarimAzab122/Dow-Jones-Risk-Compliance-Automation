import os
import json
import pandas as pd
from datetime import datetime
import paramiko
import asyncio
from app.services.dj_api import DowJonesAPIService
import logging
from logging.handlers import TimedRotatingFileHandler


SFTP_CONFIGS = {
    'input_server': {
        'hostname': os.getenv('SFTP_INPUT_HOST'),
        'port': int(os.getenv('SFTP_INPUT_PORT', '22')),
        'username': os.getenv('SFTP_INPUT_USER'),
        'password': os.getenv('SFTP_INPUT_PASS'),
        'remote_path': os.getenv('SFTP_INPUT_REMOTE_PATH'),
        'specific_filename': os.getenv('SFTP_INPUT_FILENAME')
    },
    'output_server': {
        'hostname': os.getenv('SFTP_OUTPUT_HOST'),
        'port': int(os.getenv('SFTP_OUTPUT_PORT', '22')),
        'username': os.getenv('SFTP_OUTPUT_USER'),
        'password': os.getenv('SFTP_OUTPUT_PASS'),
        'remote_path': os.getenv('SFTP_OUTPUT_REMOTE_PATH')
    }
}

LOCAL_PATHS = {
    'input':os.getenv('LOCAL_INPUT_PATH'),
    'output': os.getenv('LOCAL_OUTPUT_PATH'),
    'logs':  os.getenv('LOCAL_LOG_PATH')
}


PRIORITY_COLUMNS = [
    'peid', 'subscription_name', 'primary_name_entity_name',
    'primary_name_first_name', 'primary_name_middle_name',
    'primary_name_last_name', 'match_name', 'match_type',
    'match_id', 'gender', 'birthdates_0_day',
    'birthdates_0_month', 'birthdates_0_year'
]
def ensure_directory_exists(path):
    os.makedirs(path, exist_ok=True)

def setup_logging():

    ensure_directory_exists(LOCAL_PATHS['logs'])
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"dowjones_{timestamp}.log"
    log_path = os.path.join(LOCAL_PATHS['logs'], log_filename)
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()




def get_sftp_connection(config):
    transport = paramiko.Transport((config['hostname'], config['port']))
    transport.connect(
        username=config['username'],
        password=config['password']
    )
    return paramiko.SFTPClient.from_transport(transport)

def download_specific_file(sftp):
    ensure_directory_exists(LOCAL_PATHS['input'])
    remote_path = f"{SFTP_CONFIGS['input_server']['remote_path']}/{SFTP_CONFIGS['input_server']['specific_filename']}"
    local_path = os.path.join(LOCAL_PATHS['input'], SFTP_CONFIGS['input_server']['specific_filename'])
    
    try:
        with sftp.file(remote_path, 'rb') as remote_file:
            with open(local_path, 'wb') as local_file:
                local_file.write(remote_file.read())
                
        logger.info(f"Downloaded {SFTP_CONFIGS['input_server']['specific_filename']} to {local_path}")
        return local_path
    except Exception as e:
        logger.error(f"Failed to download file: {str(e)}")
        return None
def upload_to_servers(local_csv_path):
    """Upload CSV to both SFTP servers"""
    results = {}
    
    # Upload  (original location)
    try:
        with get_sftp_connection(SFTP_CONFIGS['input_server']) as sftp:
            remote_path = f"{SFTP_CONFIGS['input_server']['remote_path']}/DJ_Response.csv"
            sftp.put(local_csv_path, remote_path)
            results['input_server'] = True
            logger.info(f"Uploaded to server: {remote_path}")
    except Exception as e:
        results['input_server'] = False
        logger.error(f"Failed to upload to server: {str(e)}")
    
    # Upload to output server 
    try:
        with get_sftp_connection(SFTP_CONFIGS['output_server']) as sftp:
            remote_path = f"{SFTP_CONFIGS['output_server']['remote_path']}/DJ_Response.csv"
            sftp.put(local_csv_path, remote_path)
            results['output_server'] = True
            logger.info(f"Uploaded to output server: {remote_path}")
    except Exception as e:
        results['output_server'] = False
        logger.error(f"Failed to upload to output server: {str(e)}")
    
    return results

def process_json_file(filepath):
    try:
        
        try:
            with open(filepath, 'r', encoding='utf-8-sig') as f:
                data = json.load(f)
        except UnicodeDecodeError:
            
            with open(filepath, 'r', encoding='latin-1') as f:
                data = json.load(f)
        
        if not isinstance(data, dict) or 'names' not in data:
            raise ValueError("Invalid JSON format - expected {'names': [...]}")
        
        if not isinstance(data['names'], list):
            raise ValueError("'names' should be a list")
            
        return data['names']
    except Exception as e:
        logger.error(f"Error processing JSON file: {str(e)}")
        return None

def flatten_match(match):
    flattened = {}
    
    for key, value in match.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                flattened[f"{key}_{subkey}"] = subvalue
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    for subkey, subvalue in item.items():
                        flattened[f"{key}_{i}_{subkey}"] = subvalue
                else:
                    flattened[f"{key}_{i}"] = item
        else:
            flattened[key] = value
    
    return flattened

def create_output_dataframe(matches):

    flattened_matches = [flatten_match(match) for match in matches]
    df = pd.DataFrame(flattened_matches)
    
    for col in PRIORITY_COLUMNS:
        if col not in df.columns:
            df[col] = None
    
    existing_priority = [col for col in PRIORITY_COLUMNS if col in df.columns]
    other_columns = sorted([col for col in df.columns if col not in PRIORITY_COLUMNS])
    
    return df[existing_priority + other_columns]
def process_matches_response(response):
    
    try:
        if not response:
            raise ValueError("Empty API response")
            
        if 'errors' in response:
            error_msg = response['errors'][0].get('detail', 'Unknown error')
            raise ValueError(f"API error: {error_msg}")
            
        matches = []
        
        if isinstance(response.get('matches'), list):
            matches.extend(response['matches'])
        
        elif isinstance(response.get('data'), list):
            for item in response['data']:
                if isinstance(item, dict) and isinstance(item.get('attributes'), dict):
                    if isinstance(item['attributes'].get('matches'), list):
                        matches.extend(item['attributes']['matches'])
        
        elif isinstance(response.get('matches'), dict) and isinstance(response['matches'].get('data'), list):
            for item in response['matches']['data']:
                if isinstance(item, dict) and isinstance(item.get('attributes'), dict):
                    if isinstance(item['attributes'].get('matches'), list):
                        matches.extend(item['attributes']['matches'])
        
        if not matches:
            raise ValueError("No matches found in API response")
            
        return matches
        
    except Exception as e:
        logger.error(f"Error processing matches: {str(e)}")
        return None
    
def save_output_files(df, case_id):
    ensure_directory_exists(LOCAL_PATHS['output'])
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_filename = f"DJ_Response_{timestamp}.csv"
    local_path = os.path.join(LOCAL_PATHS['output'], local_filename)
    
    df.to_csv(local_path, index=False)
    logger.info(f"Saved response to {local_path}")
    
    return local_path


async def process_names(names):
    service = DowJonesAPIService()
    payload = {
        "data": {
            "attributes": {
                "case_info": {
                    "associations": [
                        {
                            "names": [{"single_string_name": name, "name_type": "PRIMARY"}],
                            "record_type": "UNKNOWN"
                        } for name in names
                    ],
                    "case_name": "screening_case",
                    "external_id": "external_id_123",
                    "owner_id": "DJ",
                    "has_alerts": True,
                    "options": {
                        "filter_content_category": ["WL"],
                        "has_to_match_low_quality_alias": True,
                        "is_indexed": False,
                        "search_type": "BROAD"
                    },
                    "score_preferences": {
                        "country": {"has_exclusions": False, "score": 0},
                        "gender": {"has_exclusions": False, "score": 0},
                        "identification_details": {"has_exclusions": False, "score": 0},
                        "industry_sector": {"has_exclusions": False, "score": 0},
                        "year_of_birth": {"has_exclusions": False, "score": 0}
                    }
                }
            },
            "type": "risk-entity-screening-cases/bulk-associations"
        }
    }
    

    creation_result = await service.create_screening_case(payload)
    case_id = creation_result["data"]["attributes"]["case_id"]
    transaction_id = creation_result["data"]["id"]
    
    logger.info(f"Case created with ID: {case_id}")
    logger.info(f"Transaction ID: {transaction_id}")
    
   
    max_transaction_retries = 50
    base_transaction_delay = 5 
    
    for attempt in range(max_transaction_retries):
        current_delay = min(base_transaction_delay * (2 ** attempt), 220)  
        
        # Get transaction details
        transaction_details = await service.get_transaction_details(case_id, transaction_id)
        transaction_status = transaction_details.get('data', {}).get('attributes', {}).get('status')
        
        logger.info(f"Transaction status: {transaction_status} (attempt {attempt + 1})")
        
        if transaction_status == "COMPLETED":
            break
        elif transaction_status in ["PENDING", "PROCESSING"]:
            if attempt < max_transaction_retries - 1:
                await asyncio.sleep(current_delay)
                continue
            else:
                raise Exception("Max retries reached while waiting for transaction to complete")
        else:
            raise Exception(f"Unexpected transaction status: {transaction_status}")

    
   
    max_retries = 50
    base_delay = 10  
    max_delay = 220  
    
    for attempt in range(max_retries):
        current_delay = min(base_delay * (2 ** attempt), max_delay)
        
        matches_response = await service.get_case_matches(case_id)
        
        if 'errors' not in matches_response:
            return case_id, transaction_id, matches_response
            
        logger.info(f"Matches still processing (attempt {attempt + 1}), waiting {current_delay} seconds...")
        await asyncio.sleep(current_delay)
    return case_id, transaction_id, matches_response

#Create an empty CSV
def create_empty_csv():
    ensure_directory_exists(LOCAL_PATHS['output'])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_filename = f"DJ_Response_{timestamp}.csv"
    local_path = os.path.join(LOCAL_PATHS['output'], local_filename)
    
    # Create empty DataFrame with just the priority columns
    df = pd.DataFrame(columns=PRIORITY_COLUMNS)
    df.to_csv(local_path, index=False)
    
    logger.info(f"Created empty response file at {local_path}")
    return local_path
async def main():
    try:
        logger.info("Starting Dow Jones screening process")
        
        with get_sftp_connection(SFTP_CONFIGS['input_server']) as sftp:
            logger.info(f"Connected to input SFTP at {SFTP_CONFIGS['input_server']['hostname']}")
            
            local_json_path = download_specific_file(sftp)
            if not local_json_path:
                return
            
            names = process_json_file(local_json_path)
            if not names:
                return
            
            case_id, transaction_id, matches_response = await process_names(names)
            
            if not matches_response or 'errors' in matches_response:
                logger.error("Failed to get matches from API")
                local_csv_path = create_empty_csv()
                upload_results = upload_to_servers(local_csv_path)
                return
            
            
            matches = process_matches_response(matches_response)
            if not matches:
                logger.warning("No matches found in API response")
                local_csv_path = create_empty_csv()
                upload_results = upload_to_servers(local_csv_path)
                return
            
            df = create_output_dataframe(matches)
            local_csv_path = save_output_files(df, case_id)
            
            # Upload for both servers
            upload_results = upload_to_servers(local_csv_path)
            
            if not all(upload_results.values()):
                logger.error("Failed to upload to one or more servers")
            
    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}", exc_info=True)
        local_csv_path = create_empty_csv()
        upload_results = upload_to_servers(local_csv_path)
    finally:
        logger.info("Processing complete")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())