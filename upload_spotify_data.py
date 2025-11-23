#!/usr/bin/env python3
"""
Script to upload Spotify streaming history from JSON to PostgreSQL database
Updated to match the actual JSON structure with processed columns from cleaning .py
"""

import json
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import os
from dotenv import load_dotenv

# Database configuration from .env
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Path to JSON file
JSON_FILE_PATH = os.getenv('JSON_FILE_PATH')

def create_table(conn):
    """Create the spotify_streams table matching the JSON structure"""
    
    # drop the old table if it exists
    drop_table_query = "DROP TABLE IF EXISTS spotify_streams;"
    
    # initialize insert table / indexes
    create_table_query = """
    CREATE TABLE spotify_streams (
        id SERIAL PRIMARY KEY,
        ms_played INTEGER,
        conn_country VARCHAR(10),
        track_name VARCHAR(500),
        artist_name VARCHAR(500),
        album_name VARCHAR(500),
        reason_start VARCHAR(100),
        reason_end VARCHAR(100),
        shuffle BOOLEAN,
        skipped BOOLEAN,
        incognito_mode BOOLEAN,
        date DATE,
        year INTEGER,
        month INTEGER,
        day_of_week VARCHAR(20),
        hour INTEGER,
        minutes_played DECIMAL(10, 2),
        is_valid_listen BOOLEAN,
        track_id VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX idx_date ON spotify_streams(date);
    CREATE INDEX idx_track_name ON spotify_streams(track_name);
    CREATE INDEX idx_artist_name ON spotify_streams(artist_name);
    CREATE INDEX idx_track_id ON spotify_streams(track_id);
    CREATE INDEX idx_year_month ON spotify_streams(year, month);
    """
    # connect and add table / indexes
    with conn.cursor() as cur:
        cur.execute(drop_table_query)
        cur.execute(create_table_query)
        conn.commit()
    print("✓ Table created/recreated")

def parse_date(date_string):
    """Parse date string to date object"""
    if date_string:
        try:
            return datetime.strptime(date_string, '%Y-%m-%d').date()
        except:
            return None
    return None

def load_json_data(file_path):
    """Load OG data from file"""
    
    print(f"Loading JSON from {file_path}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"✓ Loaded {len(data)} records")
    return data

def insert_data(conn, data, batch_size=1000):
    """Insert data into database in batches"""
    insert_query = """
    INSERT INTO spotify_streams (
        ms_played, conn_country, track_name, artist_name, album_name,
        reason_start, reason_end, shuffle, skipped, incognito_mode,
        date, year, month, day_of_week, hour, minutes_played,
        is_valid_listen, track_id
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    
    with conn.cursor() as cur:
        batch = []
        total = len(data)
        
        for i, record in enumerate(data):
            values = (
                record.get('ms_played'),
                record.get('conn_country'),
                record.get('master_metadata_track_name'),
                record.get('master_metadata_album_artist_name'),
                record.get('master_metadata_album_album_name'),
                record.get('reason_start'),
                record.get('reason_end'),
                record.get('shuffle'),
                record.get('skipped'),
                record.get('incognito_mode'),
                parse_date(record.get('date')),
                record.get('year'),
                record.get('month'),
                record.get('day_of_week'),
                record.get('hour'),
                record.get('minutes_played'),
                record.get('is_valid_listen'),
                record.get('track_id')
            )
            batch.append(values)
            
            # Insert batch when it reaches batch_size or at the end
            if len(batch) >= batch_size or i == total - 1:
                execute_batch(cur, insert_query, batch)
                conn.commit()
                print(f"  Inserted {i + 1}/{total} records ({((i + 1) / total * 100):.1f}%)")
                batch = []
    
    print("✓ All data inserted successfully")

def verify_data(conn):
    """Verify the uploaded data"""
    with conn.cursor() as cur:
        # Check total records
        cur.execute("SELECT COUNT(*) FROM spotify_streams;")
        total = cur.fetchone()[0]
        print(f"\n✓ Total records in database: {total:,}")
        
        # Check sample data
        cur.execute("""
            SELECT track_name, artist_name, minutes_played, date 
            FROM spotify_streams 
            WHERE track_name IS NOT NULL 
            LIMIT 5;
        """)
        print("\nSample records:")
        for row in cur.fetchall():
            print(f"  - {row[0]} by {row[1]} | {row[2]} min | {row[3]}")
        
        # Check for NULL values
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(track_name) as has_track,
                COUNT(minutes_played) as has_minutes,
                COUNT(date) as has_date
            FROM spotify_streams;
        """)
        row = cur.fetchone()
        print(f"\nData completeness:")
        print(f"  Total records: {row[0]:,}")
        print(f"  With track name: {row[1]:,}")
        print(f"  With minutes played: {row[2]:,}")
        print(f"  With date: {row[3]:,}")

def main():
    """Main function to orchestrate the upload process"""
    print("=" * 50)
    print("Spotify Data Upload to PostgreSQL")
    print("=" * 50)
    
    # Check if JSON file exists
    if not os.path.exists(JSON_FILE_PATH):
        print(f"Error: JSON file not found at {JSON_FILE_PATH}")
        print("Please update JSON_FILE_PATH in the script")
        return
    
    try:
        # Connect to database
        print("\nConnecting to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Connected to database")
        
        # Create table
        print("\nCreating table...")
        create_table(conn)
        
        # Load JSON data
        print("\nLoading JSON data...")
        data = load_json_data(JSON_FILE_PATH)
        
        # Insert data
        print("\nInserting data...")
        insert_data(conn, data)
        
        # Verify data
        print("\nVerifying data...")
        verify_data(conn)
        
        # Close connection
        conn.close()
        print("\n" + "=" * 50)
        print("✓ Upload completed successfully!")
        print("=" * 50)
        
    except psycopg2.Error as e:
        print(f"\n Database error: {e}")
    except json.JSONDecodeError as e:
        print(f"\n JSON parsing error: {e}")
    except Exception as e:
        print(f"\n Unexpected error: {e}")

if __name__ == "__main__":
    main()
