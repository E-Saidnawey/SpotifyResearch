from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from dotenv import load_dotenv
import os
from typing import List, Optional

load_dotenv()

app = FastAPI()

# Add CORS middleware with environment-specific origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["Content-Type"],
    max_age=3600,
)

# Database connection function
def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT')),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    return conn

# Artists endpoint - all artists alphabetically
@app.get("/api/artists")
def get_artists():
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT DISTINCT artist_name FROM spotify_streams ORDER BY artist_name")
    
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {"Data": [row[0] for row in results]}

# Top artists by listening time (with optional date filtering)
@app.get("/api/artists/top")
def get_top_artists(
    limit: int = Query(20, ge=1, le=100),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    conn = get_db_connection()
    cur = conn.cursor()
    
    where_clauses = []
    params = []
    
    if start_date:
        where_clauses.append("date >= %s")
        params.append(start_date)
    
    if end_date:
        where_clauses.append("date <= %s")
        params.append(end_date)
    
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    query = f"""
        SELECT artist_name, SUM(minutes_played) as total_minutes
        FROM spotify_streams
        WHERE {where_sql}
        GROUP BY artist_name
        ORDER BY total_minutes DESC
        LIMIT %s
    """
    
    params.append(limit)
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {"Data": [row[0] for row in results]}

# Year endpoint
@app.get("/api/years")
def get_years():
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT DISTINCT year FROM spotify_streams ORDER BY year")
    
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {"Data": [row[0] for row in results]}
    
# Column Endpoint 
@app.get("/api/columns")
def get_columns():
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'spotify_streams'
        AND column_name NOT IN ('id', 'created_at', 'date', 'ms_played', 'minutes_played')
        ORDER BY column_name
    """)
    
    results = cur.fetchall()
    cur.close()
    conn.close()
    
    return {"Data": [row[0] for row in results]}
    
# Track endpoint
@app.get("/api/tracks")
def get_tracks():
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT DISTINCT track_name FROM spotify_streams ORDER BY track_name")
    
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {"Data": [row[0] for row in results]}
    
# Album endpoint
@app.get("/api/albums")
def get_albums():
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT DISTINCT album_name FROM spotify_streams ORDER BY album_name")
    
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {"Data": [row[0] for row in results]}


@app.get("/api/aggregate")
def aggregate_data(
    group_by: str = Query(..., description="Comma-separated columns to group by"),
    filter_artists: Optional[str] = Query(None, description="Comma-separated artist names"),
    filter_years: Optional[str] = Query(None, description="Comma-separated years"),
    limit: int = Query(50, ge=1, le=1000),
    top_per_group: bool = Query(False, description="Return only top result per group")
):
    # Parse the comma-separated string
    group_by_columns = [col.strip() for col in group_by.split(',')]
    
    # Whitelist each column
    ALLOWED_GROUP_BY = ['artist_name', 'track_name', 'album_name', 
                        'year', 'month', 'day_of_week', 'hour']
    
    for col in group_by_columns:
        if col not in ALLOWED_GROUP_BY:
            raise HTTPException(status_code=400, detail=f"Invalid column: {col}")
    
    # Build the SELECT and GROUP BY parts
    select_columns = ", ".join(group_by_columns)
    group_by_clause = ", ".join(group_by_columns)
    
    # Build WHERE clause
    where_clauses = []
    params = []
    
    # Handle multiple artists filter
    if filter_artists:
        artist_list = [a.strip() for a in filter_artists.split(',')]
        placeholders = ', '.join(['%s'] * len(artist_list))
        where_clauses.append(f"artist_name IN ({placeholders})")
        params.extend(artist_list)
    
    # Handle multiple years filter
    if filter_years:
        year_list = [int(y.strip()) for y in filter_years.split(',')]
        placeholders = ', '.join(['%s'] * len(year_list))
        where_clauses.append(f"year IN ({placeholders})")
        params.extend(year_list)
    
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    # Build query with multiple columns
    if top_per_group and len(group_by_columns) > 1:
        # Use window function to get top result per group
        # Assuming the first column is the primary grouping
        primary_group = group_by_columns[0]
        other_columns = group_by_columns[1:]
        
        query = f"""
            WITH ranked AS (
                SELECT {select_columns}, 
                       SUM(minutes_played) as total_minutes,
                       COUNT(*) as play_count,
                       ROW_NUMBER() OVER (PARTITION BY {primary_group} ORDER BY SUM(minutes_played) DESC) as rn
                FROM spotify_streams
                WHERE {where_sql}
                GROUP BY {group_by_clause}
            )
            SELECT {select_columns}, total_minutes, play_count
            FROM ranked
            WHERE rn = 1
            ORDER BY total_minutes DESC
            LIMIT %s
        """
    else:
        query = f"""
            SELECT {select_columns}, 
                   SUM(minutes_played) as total_minutes,
                   COUNT(*) as play_count
            FROM spotify_streams
            WHERE {where_sql}
            GROUP BY {group_by_clause}
            ORDER BY total_minutes DESC
            LIMIT %s
        """
    
    params.append(limit)
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    conn.close()
    
    # Build response dynamically based on number of group_by columns
    data = []
    for row in results:
        item = {}
        for i, col in enumerate(group_by_columns):
            item[col] = row[i]
        item['total_minutes'] = float(row[len(group_by_columns)])
        item['play_count'] = row[len(group_by_columns) + 1]
        data.append(item)
    
    return {"data": data}
