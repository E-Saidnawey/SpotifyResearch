from fastapi import FastAPI, Query, HTTPException
import psycopg2
from dotenv import load_dotenv
import os


load_dotenv()

app = FastAPI()

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

# Artists endpoint
@app.get("/api/artists")
def get_artists():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Your SQL query
    cur.execute("SELECT DISTINCT artist_name FROM spotify_streams ORDER BY artist_name")
    
    # Fetch results
    results = cur.fetchall()
    
    # Close connection
    cur.close()
    conn.close()
    
    # Return as JSON
    return {"Data": [row[0] for row in results]}
    
    
# Year endpoint
@app.get("/api/years")
def get_years():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Your SQL query
    cur.execute("SELECT DISTINCT year FROM spotify_streams ORDER BY year")
    
    # Fetch results
    results = cur.fetchall()
    
    # Close connection
    cur.close()
    conn.close()
    
    # Return as JSON
    return {"Data": [row[0] for row in results]}
    
# Column Endpoint 
@app.get("/api/columns")
def get_columns():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Exclude id and created at, we can't group by those
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
def get_artists():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Your SQL query
    cur.execute("SELECT DISTINCT track_name FROM spotify_streams ORDER BY track_name")
    
    # Fetch results
    results = cur.fetchall()
    
    # Close connection
    cur.close()
    conn.close()
    
    # Return as JSON
    return {"Data": [row[0] for row in results]}
    
# Album endpoint
@app.get("/api/albums")
def get_artists():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Your SQL query
    cur.execute("SELECT DISTINCT album_name FROM spotify_streams ORDER BY album_name")
    
    # Fetch results
    results = cur.fetchall()
    
    # Close connection
    cur.close()
    conn.close()
    
    # Return as JSON
    return {"Data": [row[0] for row in results]}


@app.get("/api/aggregate")
def aggregate_data(
    group_by: str = Query(..., description="Comma-separated columns to group by"),
    filter_artist: str = Query(None),
    filter_year: int = Query(None),
    limit: int = Query(100, ge=1, le=1000)
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
    
    # Build WHERE clause (same as before)
    where_clauses = []
    params = []
    
    if filter_artist:
        where_clauses.append("artist_name = %s")
        params.append(filter_artist)
    
    if filter_year:
        where_clauses.append("year = %s")
        params.append(filter_year)
    
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    # Build query with multiple columns
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
