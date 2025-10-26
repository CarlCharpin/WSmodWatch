import praw
import sqlite3
import time
import re
import json
import logging
import sys
import configparser


# --- 1. CONFIGURATION ---

config = configparser.ConfigParser()

try:
    config.read('config.ini')
    
    # Load Reddit Secrets
    reddit_config = config['REDDIT_SECRETS']
    CLIENT_ID = reddit_config['CLIENT_ID']
    CLIENT_SECRET = reddit_config['CLIENT_SECRET']
    USER_AGENT = reddit_config['USER_AGENT']
    # .get() is safer for optional values, providing a fallback (like your old code)
    USERNAME = reddit_config.get('USERNAME', '') 
    PASSWORD = reddit_config.get('PASSWORD', '') 

    # Load App Settings
    app_config = config['APP_SETTINGS']
    SUBREDDIT_NAME = app_config['SUBREDDIT_NAME']
    DB_NAME = app_config['DB_NAME']
    LOG_FILE = app_config['LOG_FILE']

except FileNotFoundError:
    logging.critical("ERROR: The 'config.ini' file was not found. Please create it.")
    sys.exit(1)
except KeyError as e:
    logging.critical(f"ERROR: Missing a required key in 'config.ini'. Please add: {e}")
    sys.exit(1)


# Set up logging (This section remains the same, but now uses the LOG_FILE variable from the .ini)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(LOG_FILE),
                        logging.StreamHandler(sys.stdout)
                    ])

# --- 2. PRAW & DB SETUP FUNCTIONS ---

def get_reddit_instance():
    """Initializes and returns the PRAW Reddit instance."""
    try:
        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            user_agent=USER_AGENT,
            username=USERNAME,
            password=PASSWORD
        )
        # Verify read-only access if no credentials provided
        if not USERNAME and not reddit.read_only:
             logging.warning("PRAW instance is not read-only. Ensure credentials are set correctly.")
        logging.info("PRAW connection established successfully.")
        return reddit
    except Exception as e:
        logging.error(f"PRAW initialization failed: {e}")
        sys.exit(1)

def initialize_db():
    """Creates the SQLite database and the threads table if they do not exist."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS threads (
            post_id TEXT PRIMARY KEY,
            title TEXT,
            selftext TEXT,
            created_utc REAL,
            status TEXT DEFAULT 'ACTIVE',
            removal_category TEXT,
            extracted_tickers TEXT
        )
    """)
    conn.commit()
    conn.close()
    logging.info(f"Database '{DB_NAME}' initialized successfully.")

# --- 3. HARVESTER FUNCTION ---

def harvest_new_threads(reddit, subreddit_name, conn):
    """
    Pillar 1: Fetches new posts and inserts them into the database.
    Now logs pulled count and inserted count.
    """
    logging.info(f"Starting Harvester on r/{subreddit_name}...")
    subreddit = reddit.subreddit(subreddit_name)
    cursor = conn.cursor()
    
    pulled_count = 0
    total_inserted = 0
    
    try:
        # Fetch the latest 100 posts from the 'new' listing (Reddit's max limit per request)
        for submission in subreddit.new(limit=100):
            pulled_count += 1
            
            # Skip if author is deleted (PRAW returns None for submission.author)
            if submission.author is None: 
                author = "[Deleted]"
            else:
                author = submission.author.name

            data = (
                submission.id,
                submission.title,
                submission.selftext or "", 
                author,
                submission.created_utc
            )

            # Use INSERT OR IGNORE to skip duplicates (where post_id is the primary key)
            cursor.execute("""
                INSERT OR IGNORE INTO threads (post_id, title, selftext, author_name, created_utc) 
                VALUES (?, ?, ?, ?, ?)
            """, data)
            
            # Check how many rows were affected by the last insert command
            if cursor.rowcount > 0:
                total_inserted += 1

        conn.commit()
        logging.info(f"Harvester finished: Pulled {pulled_count} posts from API. Inserted {total_inserted} new unique threads (duplicates ignored).")
        
    except Exception as e:
        logging.error(f"Harvester error: {e}")
        conn.rollback()

# --- 4. CHECKER FUNCTION (IMPROVED TO CHECK MOST RECENT) ---

def check_for_deletions(reddit, conn):
    """
    Pillar 3: Checks currently ACTIVE posts for removal/deletion using the info endpoint.
    """
    logging.info("Starting Checker for deletions...")
    cursor = conn.cursor()
    
    # 1. Select up to 100 ACTIVE post IDs to check
    # --- IMPROVEMENT: Added 'ORDER BY created_utc DESC' ---
    # This selects the 100 NEWEST active posts.
    cursor.execute("SELECT post_id FROM threads WHERE status = 'ACTIVE' ORDER BY created_utc DESC LIMIT 100")
    active_ids = [f"t3_{row[0]}" for row in cursor.fetchall()]

    if not active_ids:
        logging.info("Checker: No active posts to check.")
        return

    try:
        # 2. Query the Reddit API for the status of these IDs
        # reddit.info returns a generator for active submissions
        active_submissions = list(reddit.info(fullnames=active_ids))

        # 3. Comparison Logic: Build a set of IDs that were returned (i.e., still active)
        returned_ids = {sub.fullname for sub in active_submissions}
        
        # Identify deleted posts (IDs requested but not returned)
        deleted_ids = set(active_ids) - returned_ids

        # 4. Update DB: Mark truly deleted/removed posts
        if deleted_ids:
            # 1. Convert fullnames (e.g., 't3_123abc') to bare IDs (e.g., '123abc')
            bare_deleted_ids = tuple([post_fullname.split('_', 1)[1] for post_fullname in deleted_ids])
            
            deleted_placeholders = ','.join('?' * len(bare_deleted_ids))
            
            # 2. Update the query to use the correct 'post_id' column
            cursor.execute(f"""
                UPDATE threads 
                SET status = 'REMOVED', removal_category = 'MOD_OR_USER_REMOVED'
                WHERE post_id IN ({deleted_placeholders})
            """, bare_deleted_ids)
            
            logging.warning(f"Checker: Found and flagged {len(bare_deleted_ids)} deleted/removed posts.")

        # Update removal_category for any posts that might have been visible 
        # but had their removal_category set by a mod (less critical, but good practice)
        for sub in active_submissions:
            removal_status = getattr(sub, 'removed_by_category', None)
            if removal_status in ('moderator', 'deleted', 'automoderator'):
                # Note: This case is rare when using info() endpoint, but included for completeness.
                cursor.execute("UPDATE threads SET status = 'REMOVED', removal_category = ? WHERE post_id = ?", 
                               (removal_status, sub.id))

        conn.commit()
        logging.info("Checker finished. Database updated.")

    except Exception as e:
        logging.error(f"Checker error: {e}")
        conn.rollback()

# --- 5. ANALYSIS FUNCTION (REVISED REGEX) ---

def analyze_removed_threads(conn):
    """
    Pillar 4: Analyzes threads marked as 'REMOVED' for tickers and updates status.
    """
    logging.info("Starting Analysis for removed threads...")
    cursor = conn.cursor()

    # 1. Select threads pending analysis
    cursor.execute("SELECT post_id, title, selftext FROM threads WHERE status = 'REMOVED'")
    removed_posts = cursor.fetchall()

    if not removed_posts:
        logging.info("Analysis: No removed posts found to analyze.")
        return

    # --- CHANGED: More robust RegEx ---
    # This now finds:
    # 1. 3-4 letter ALL CAPS words (e.g., BTQ, BYND)
    # 2. 3-4 letter words preceded by a $ (case-insensitive, e.g., $aapl, $bynd)
    TICKER_REGEX = r'(?:\b[A-Z]{3,4}\b|\$[a-zA-Z]{3,4}\b)'
    
    analyzed_count = 0
    
    for post_id, title, selftext in removed_posts:
        full_text = f"{title} {selftext}"
        
        # Find all matches (e.g., ['BTQ', '$bynd', '$aapl'])
        found_tickers = re.findall(TICKER_REGEX, full_text)
        
        # --- CHANGED: Updated cleanup logic ---
        # Strip the '$' (if it exists) and convert all to uppercase for consistency
        cleaned_tickers = list(set([t.lstrip('$').upper() for t in found_tickers]))
        
        # 2. Store the list of found tickers (using JSON to serialize list)
        if cleaned_tickers:
            tickers_json = json.dumps(cleaned_tickers)
            
            cursor.execute("""
                UPDATE threads 
                SET extracted_tickers = ?, status = 'ANALYZED'
                WHERE post_id = ?
            """, (tickers_json, post_id))
            analyzed_count += 1
        else:
            # 3. Update status even if no tickers found, so we don't re-analyze
            cursor.execute("UPDATE threads SET status = 'ANALYZED' WHERE post_id = ?", (post_id,))

    conn.commit()
    logging.info(f"Analysis finished. {analyzed_count} threads had tickers extracted.")


# --- 6. MAIN LOOP (REVISED FOR PERFORMANCE) ---

def main_loop(reddit):
    """Manages the scheduling and execution of the three pillars."""
    
    # Timing markers (in seconds since epoch)
    last_harvest = 0
    last_check = 0
    last_analysis = 0
    
    # Target intervals (in seconds)
    HARVEST_INTERVAL = 10
    CHECK_INTERVAL = 60  # 1 minute (Corrected comment)
    ANALYSIS_INTERVAL = 1800 # 30 minutes

    conn = None # We will manage the connection state here

    try:
        while True:
            current_time = time.time()
            
            try:
                # --- NEW: Connection Management ---
                # If connection is lost (or not yet established), reconnect.
                if conn is None:
                    logging.info("Establishing database connection...")
                    conn = sqlite3.connect(DB_NAME)
                    logging.info("Database connection successful.")

                # --- Harvester ---
                if current_time - last_harvest >= HARVEST_INTERVAL:
                    harvest_new_threads(reddit, SUBREDDIT_NAME, conn)
                    last_harvest = current_time

                # --- Checker ---
                if current_time - last_check >= CHECK_INTERVAL:
                    check_for_deletions(reddit, conn)
                    last_check = current_time
                
                # --- Analyzer ---
                if current_time - last_analysis >= ANALYSIS_INTERVAL:
                    analyze_removed_threads(conn)
                    last_analysis = current_time

                # --- Sleep Logic (Unchanged) ---
                sleep_time = min(
                    HARVEST_INTERVAL - (current_time - last_harvest),
                    CHECK_INTERVAL - (current_time - last_check),
                    ANALYSIS_INTERVAL - (current_time - last_analysis),
                    10 # Max sleep time
                )
                
                if sleep_time > 0:
                    logging.debug(f"Sleeping for {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)

            # --- CHANGED: Error Handling ---

            except praw.exceptions.APIException as e:
                logging.error(f"PRAW API Rate Limit Hit or Error: {e}. Sleeping for 60 seconds.")
                time.sleep(60)
            
            except sqlite3.Error as e:
                # Database error (e.g., "database is locked")
                logging.error(f"Database Error: {e}. Closing connection and will retry.")
                if conn:
                    conn.close()
                conn = None # Signal to reconnect on the next loop
                time.sleep(10) # Wait 10s before retrying DB operations
            
            except Exception as e:
                logging.critical(f"A critical unhandled error occurred: {e}. Exiting script.")
                break # Exit the main 'while True' loop

    except KeyboardInterrupt:
        logging.info("Script interrupted by user. Shutting down.")
    
    finally:
        # This 'finally' catches the exit from 'break' or 'KeyboardInterrupt'
        if conn:
            logging.info("Closing database connection.")
            conn.close()


if __name__ == "__main__":
    print("--- Reddit Deletion Monitor Script ---")
    print(f"Monitoring subreddit: r/{SUBREDDIT_NAME}")
    print(f"Database file: {DB_NAME}")
    
    # Step 1: Initialize resources
    initialize_db()
    reddit = get_reddit_instance()
    
    # Step 2: Start the continuous loop
    # The loop function itself now manages the DB connection lifecycle.
    main_loop(reddit)

    print("--- Script has terminated. ---")