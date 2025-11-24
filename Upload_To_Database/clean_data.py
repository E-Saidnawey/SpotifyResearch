from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import os
import glob
import json

"""
Simplified class to load and clean Spotify streaming history data.
Returns cleaned DataFrame for use in other scripts.
"""
    
def load_json_files(json_folder_path):
    """
    Load all JSON files from the specified folder.
    
    Returns:
        list: Raw data from all JSON files
    """
    raw_data = []
    json_files = list(glob.glob(os.path.join(json_folder_path, '*.json')))
    
    
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {json_folder_path}")
    
    print(f"Found {len(json_files)} JSON file(s)")
    
    for file_path in json_files:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Handle both single dict and list of dicts
            if isinstance(data, list):
                raw_data.extend(data)
            else:
                raw_data.append(data)
    
    print(f"Loaded {len(raw_data)} streaming records")
    return raw_data
    
def clean_and_organize(data):
    """
    Clean and organize the raw JSON data into a structured DataFrame.
    
    Returns:
        pandas.DataFrame: Cleaned and organized data
    """
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    print(f"Initial data shape: {df.shape}")
    
    # Convert timestamp to datetime
    df['ts'] = pd.to_datetime(df['ts'])
    
    # Extract useful time components
    df['date'] = df['ts'].dt.date
    df['year'] = df['ts'].dt.year
    df['month'] = df['ts'].dt.month
    df['day_of_week'] = df['ts'].dt.day_name()
    df['hour'] = df['ts'].dt.hour
    
    # Convert ms_played to minutes
    df['minutes_played'] = df['ms_played'] / 60000
    
    # Filter out tracks with 0 playtime (skipped immediately)
    df['is_valid_listen'] = df['ms_played'] > 0
    
    # Create a clean track identifier
    df['track_id'] = df['master_metadata_track_name'] + ' - ' + \
                          df['master_metadata_album_artist_name']
    
    # Handle missing values
    df['master_metadata_track_name'] = df['master_metadata_track_name'].fillna('Unknown Track')
    df['master_metadata_album_artist_name'] = df['master_metadata_album_artist_name'].fillna('Unknown Artist')
    df['master_metadata_album_album_name'] = df['master_metadata_album_album_name'].fillna('Unknown Album')
    
    # Delete values from unwanted columns
    index = df['episode_name'].isna()
    df = df[index]
    df = df.drop(columns=['platform', 'ip_addr', 'audiobook_title', 'audiobook_uri', 'audiobook_chapter_uri', 'audiobook_chapter_title', 'episode_name', 'episode_show_name', 'spotify_track_uri', 'spotify_episode_uri', 'offline_timestamp', 'offline'])
    
    print(f"Cleaned data shape: {df.shape}")
    print(f"Total listening time: {df['minutes_played'].sum():.2f} minutes ({df['minutes_played'].sum()/60:.2f} hours)")
    return df
    
def write_to_json(df):
    df['date'] = df['date'].astype(str)
    
    df.to_json(os.getenv('CLEAN_JSON_DATA'), orient='records', indent=4)
    
    print(f"DataFrame successfully written to {os.getenv('CLEAN_JSON_DATA')}")
    return df

def main():
    """Main function to orchestrate the upload process"""
    print("=" * 50)
    print("Spotify Data Upload to PostgreSQL")
    print("=" * 50)
    
    load_dotenv()
    
    json_folder_path = os.getenv('INPUT_JSON_FOLDER')
    
    # Check if JSON file exists
    if not os.path.exists(json_folder_path):
        print(f"Error: JSON file not found at {json_folder_path}")
        print("Please update JSON_FILE_PATH in the script")
        return
    
    try:
        print("-" * 20)
        print('Running JSON load')
        df = load_json_files(json_folder_path)
        
        print("-" * 20)
        print('Cleaning JSON')
        df = clean_and_organize(df)
        
        print("-" * 50)
        print('Saving JSON')
        df = write_to_json(df)
        
    except Exception as e:
        print(f"\n Unexpected error: {e}")
        
    return

# Example usage
if __name__ == "__main__":
    main()
