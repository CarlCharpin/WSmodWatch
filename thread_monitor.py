import praw
import sqlite3
import time
import re
import json
import logging
import sys
import configparser
from datetime import datetime, timedelta

# NEW DEPENDENCIES for Visualization
try:
    import matplotlib.pyplot as plt
    import numpy as np
    from wordcloud import WordCloud
except ImportError:
    print("Warning: Missing required visualization libraries (matplotlib, numpy, wordcloud). Please install them using: pip install matplotlib numpy wordcloud")
    # Define dummy functions/classes to allow the rest of the script to run without crashing
    WordCloud = object
    plt = None
    np = None


# --- 1. CONFIGURATION LOADING ---

config = configparser.ConfigParser()

try:
    config.read('config.ini')
    
    # Load Reddit Secrets
    reddit_config = config['REDDIT_SECRETS']
    CLIENT_ID = reddit_config['CLIENT_ID']
    CLIENT_SECRET = reddit_config['CLIENT_SECRET']
    USER_AGENT = reddit_config['USER_AGENT']
    USERNAME = reddit_config.get('USERNAME', '') 
    PASSWORD = reddit_config.get('PASSWORD', '') 

    # Load App Settings
    app_config = config['APP_SETTINGS']
    SUBREDDIT_NAME = app_config['SUBREDDIT_NAME']
    DB_NAME = app_config['DB_NAME']
    LOG_FILE = app_config['LOG_FILE']
    TICKER_FILTER_FILE = app_config.get('TICKER_FILTER_FILE', 'ticker_allow_list.txt')

except FileNotFoundError:
    logging.critical("ERROR: The 'config.ini' file was not found. Please create it.")
    sys.exit(1)
except KeyError as e:
    logging.critical(f"ERROR: Missing a required key in 'config.ini'. Please add: {e}")
    sys.exit(1)


# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(LOG_FILE),
                        logging.StreamHandler(sys.stdout)
                    ])

# --- GLOBAL FILTER VARIABLE ---
ALLOWED_TICKERS_SET = set()


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
            author_name TEXT,
            created_utc REAL,
            removed_utc REAL,
            initial_score INTEGER,
            num_comments_initial INTEGER,
            link_flair TEXT,
            status TEXT DEFAULT 'ACTIVE',
            removal_category TEXT,
            extracted_tickers TEXT
        )
    """)
    conn.commit()
    conn.close()
    logging.info(f"Database '{DB_NAME}' initialized successfully.")

def load_allowed_tickers(filter_filepath):
    """Loads the external list of pre-approved, high-cap tickers."""
    global ALLOWED_TICKERS_SET
    try:
        with open(filter_filepath, 'r', encoding='utf-8') as f:
            ALLOWED_TICKERS_SET = {line.strip().upper() for line in f if line.strip()}
        
        if not ALLOWED_TICKERS_SET:
            logging.warning("WARNING: Ticker Allow List is EMPTY. All extracted tickers will be rejected.")
        else:
            logging.info(f"Loaded {len(ALLOWED_TICKERS_SET)} verified tickers for filtering.")
            
    except FileNotFoundError:
        logging.critical(f"FATAL: Ticker filter file not found at path: {filter_filepath}. Analysis cannot be performed accurately.")
    except Exception as e:
        logging.critical(f"FATAL: Error loading ticker filter file: {e}")
        
    return ALLOWED_TICKERS_SET


# --- 3. HARVESTER FUNCTION ---

def harvest_new_threads(reddit, subreddit_name, conn):
    """
    Pillar 1: Fetches new posts and inserts them into the database.
    """
    logging.info(f"Starting Harvester on r/{subreddit_name}...")
    subreddit = reddit.subreddit(subreddit_name)
    cursor = conn.cursor()
    
    pulled_count = 0
    total_inserted = 0
    
    try:
        for submission in subreddit.new(limit=100):
            pulled_count += 1
            
            author = submission.author.name if submission.author else "[Deleted]"
            link_flair = submission.link_flair_text or ""

            data = (
                submission.id,
                submission.title,
                submission.selftext or "", 
                author,
                submission.created_utc,
                submission.score,
                submission.num_comments,
                link_flair
            )

            # Use INSERT OR IGNORE to skip duplicates (where post_id is the primary key)
            cursor.execute("""
                INSERT OR IGNORE INTO threads (post_id, title, selftext, author_name, created_utc, 
                                             initial_score, num_comments_initial, link_flair) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, data)
            
            if cursor.rowcount > 0:
                total_inserted += 1

        conn.commit()
        logging.info(f"Harvester finished: Pulled {pulled_count} posts from API. Inserted {total_inserted} new unique threads (duplicates ignored).")
        
    except Exception as e:
        logging.error(f"Harvester error: {e}")
        conn.rollback()

# --- 4. CHECKER FUNCTION ---

def check_for_deletions(reddit, conn):
    """
    Pillar 3: Checks currently ACTIVE posts for removal/deletion using the info endpoint.
    """
    logging.info("Starting Checker for deletions...")
    cursor = conn.cursor()
    
    # Select up to 100 ACTIVE post IDs to check, prioritized by newest
    cursor.execute("SELECT post_id FROM threads WHERE status = 'ACTIVE' ORDER BY created_utc DESC LIMIT 100")
    active_ids = [f"t3_{row[0]}" for row in cursor.fetchall()]

    if not active_ids:
        logging.info("Checker: No active posts to check.")
        return

    try:
        active_submissions = list(reddit.info(fullnames=active_ids))
        returned_ids = {sub.fullname for sub in active_submissions}
        deleted_ids = set(active_ids) - returned_ids

        if deleted_ids:
            bare_deleted_ids = tuple([post_fullname.split('_', 1)[1] for post_fullname in deleted_ids])
            deleted_placeholders = ','.join('?' * len(bare_deleted_ids))
            current_utc = time.time()
            
            cursor.execute(f"""
                UPDATE threads 
                SET status = 'REMOVED', removal_category = 'MOD_OR_USER_REMOVED', removed_utc = {current_utc}
                WHERE post_id IN ({deleted_placeholders})
            """, bare_deleted_ids)
            
            logging.warning(f"Checker: Found and flagged {len(bare_deleted_ids)} deleted/removed posts.")

        for sub in active_submissions:
            removal_status = getattr(sub, 'removed_by_category', None)
            if removal_status in ('moderator', 'deleted', 'automoderator'):
                cursor.execute("""
                    UPDATE threads SET status = 'REMOVED', removal_category = ?, removed_utc = ? 
                    WHERE post_id = ?
                """, (removal_status, time.time(), sub.id))

        conn.commit()
        
        cursor.execute("SELECT COUNT(post_id) FROM threads WHERE status != 'ACTIVE'")
        total_deleted = cursor.fetchone()[0]
        logging.info(f"Checker finished. Total deleted threads in DB: {total_deleted}")

    except Exception as e:
        logging.error(f"Checker error: {e}")
        conn.rollback()

# --- 5. ANALYSIS FUNCTION ---

def analyze_removed_threads(conn):
    """
    Pillar 4: Analyzes threads marked as 'REMOVED' for tickers and updates status.
    Filters extracted tickers against the global ALLOWED_TICKERS_SET.
    """
    logging.info("Starting Analysis for removed threads...")
    cursor = conn.cursor()

    cursor.execute("SELECT post_id, title, selftext FROM threads WHERE status = 'REMOVED'")
    removed_posts = cursor.fetchall()

    if not removed_posts:
        logging.info("Analysis: No removed posts found to analyze.")
        return
        
    if not ALLOWED_TICKERS_SET:
        logging.warning("Analysis skipped: The ALLOWED_TICKERS_SET is empty. Cannot filter candidates.")
        return

    # Ticker Extraction RegEx
    TICKER_REGEX = r'(?:\b[A-Z]{2,5}\b|\$[a-zA-Z]{2,5}\b)' # Adjusted to 2-5 chars for more coverage
    
    analyzed_count = 0
    
    for post_id, title, selftext in removed_posts:
        full_text = f"{title} {selftext}"
        
        raw_candidates = re.findall(TICKER_REGEX, full_text)
        
        verified_tickers = set()
        
        for t in raw_candidates:
            cleaned_ticker = t.lstrip('$').upper()
            
            # Validation: Only keep tickers found in the pre-approved list
            if cleaned_ticker in ALLOWED_TICKERS_SET:
                verified_tickers.add(cleaned_ticker)
        
        if verified_tickers:
            tickers_json = json.dumps(list(verified_tickers))
            
            cursor.execute("""
                UPDATE threads 
                SET extracted_tickers = ?, status = 'ANALYZED'
                WHERE post_id = ?
            """, (tickers_json, post_id))
            analyzed_count += 1
        else:
            cursor.execute("UPDATE threads SET status = 'ANALYZED' WHERE post_id = ?", (post_id,))

    conn.commit()
    logging.info(f"Analysis finished. {analyzed_count} threads had verified tickers extracted.")

# --- 6. REPORTING AND VISUALIZATION FUNCTIONS ---

def calculate_weighted_scores(conn, start_utc):
    """
    Calculates Ticker Score = Total Mentions * Unique Authors for all relevant posts
    within the given time window.
    """
    cursor = conn.cursor()
    
    # Query to fetch all analyzed tickers and their authors within the time window
    cursor.execute("""
        SELECT extracted_tickers, author_name 
        FROM threads 
        WHERE status = 'ANALYZED' AND removed_utc >= ? AND extracted_tickers IS NOT NULL
    """, (start_utc,))
    
    results = cursor.fetchall()

    ticker_mentions = {}
    ticker_unique_authors = {}
    
    for tickers_json, author_name in results:
        try:
            # Safely load the JSON array of tickers
            tickers = json.loads(tickers_json)
        except (TypeError, json.JSONDecodeError):
            continue

        for ticker in tickers:
            # 1. Total Mentions Count
            ticker_mentions[ticker] = ticker_mentions.get(ticker, 0) + 1
            
            # 2. Unique Author Count
            if ticker not in ticker_unique_authors:
                ticker_unique_authors[ticker] = set()
            ticker_unique_authors[ticker].add(author_name)

    # 3. Calculate Final Weighted Score: Mentions * Unique Authors Count
    weighted_scores = {}
    for ticker, count in ticker_mentions.items():
        unique_authors_count = len(ticker_unique_authors[ticker])
        weighted_score = count * unique_authors_count
        weighted_scores[ticker] = weighted_score
        
    return weighted_scores


def generate_word_cloud_report(conn, time_window_seconds, timeframe_label):
    """
    Generates and saves a weighted word cloud visualization.
    """
    
    if plt is None or WordCloud is object:
        logging.error("Reporting failed: Missing visualization dependencies.")
        return

    # Calculate the start time (rolling window)
    start_utc = time.time() - time_window_seconds
    
    # 1. Calculate Weighted Scores
    weighted_scores = calculate_weighted_scores(conn, start_utc)
    
    if not weighted_scores:
        logging.info(f"Report ({timeframe_label}): No analyzed data found for the last {timeframe_label} window.")
        return

    # 2. Generate the Word Cloud
    wc = WordCloud(
        width=1000, 
        height=600, 
        background_color="white", 
        colormap='viridis',
        min_font_size=10
    )
    
    # The generate_from_frequencies method uses the weighted scores as the input
    wc.generate_from_frequencies(weighted_scores)
    
    # 3. Save the PNG file
    
    # Generate timestamped filename
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"wordcloud reports/{timeframe_label.lower()}_{timestamp_str}.png"
    
    # Use Matplotlib to add a title and save
    plt.figure(figsize=(10, 6))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.title(f"Deleted Ticker Activity - Last {timeframe_label} (Weighted by Unique Users)", fontsize=16)
    
    try:
        plt.savefig(filename, bbox_inches='tight')
        plt.close()
        logging.info(f"Report generated successfully: {filename}")
    except Exception as e:
        logging.error(f"Error saving word cloud image: {e}")


# --- 7. MAIN LOOP (WITH REPORT SCHEDULING) ---

def main_loop(reddit):
    """Manages the scheduling and execution of the three pillars and reporting."""
    
    # Timing markers (in seconds since epoch)
    last_harvest = 0
    last_check = 0
    last_analysis = 0
    
    # Report timing markers
    last_report_hourly = 0
    last_report_daily_1 = 0 # First daily run (e.g., mid-day)
    last_report_daily_2 = 0 # Second daily run (e.g., mid-night)
    last_report_weekly = 0

    # Target intervals (in seconds)
    HARVEST_INTERVAL = 1
    CHECK_INTERVAL = 30 
    ANALYSIS_INTERVAL = 60 # 30 min 
    
    # REPORTING INTERVALS
    REPORT_HOURLY_INTERVAL = 3600 # 1 hour
    REPORT_DAILY_INTERVAL_1 = 43200 # 12 hours (For the two overlapping 24hr runs)
    REPORT_DAILY_INTERVAL_2 = 43200 # 12 hours (For the two overlapping 24hr runs)
    REPORT_WEEKLY_INTERVAL = 86400 # 24 hours

    # Time windows for the reports (in seconds)
    WINDOW_HOURLY = 3600
    WINDOW_DAILY = 86400 # 24 hours
    WINDOW_WEEKLY = 604800 # 7 days
    
    conn = None 

    try:
        while True:
            current_time = time.time()
            
            try:
                if conn is None:
                    logging.info("Establishing database connection...")
                    conn = sqlite3.connect(DB_NAME)
                    logging.info("Database connection successful.")

                # --- Core Pillars ---
                if current_time - last_harvest >= HARVEST_INTERVAL:
                    harvest_new_threads(reddit, SUBREDDIT_NAME, conn)
                    last_harvest = current_time

                if current_time - last_check >= CHECK_INTERVAL:
                    check_for_deletions(reddit, conn)
                    last_check = current_time
                
                if current_time - last_analysis >= ANALYSIS_INTERVAL:
                    analyze_removed_threads(conn)
                    last_analysis = current_time
                
                # --- Reporting Scheduling (Hourly, Daily, Weekly) ---

                # 1. Hourly Report (Last 60 minutes) - Runs every hour
                if current_time - last_report_hourly >= REPORT_HOURLY_INTERVAL:
                    generate_word_cloud_report(conn, WINDOW_HOURLY, "Hourly")
                    last_report_hourly = current_time

                # 2. Daily Report 1 (Last 24 hours) - Runs every 12 hours
                if current_time - last_report_daily_1 >= REPORT_DAILY_INTERVAL_1:
                    generate_word_cloud_report(conn, WINDOW_DAILY, "Daily_Run1")
                    last_report_daily_1 = current_time
                    
                # 3. Daily Report 2 (Last 24 hours) - Runs every 12 hours, offset by 6 hours from Run 1
                if current_time - last_report_daily_2 >= REPORT_DAILY_INTERVAL_2 and (current_time - last_report_daily_1) > (REPORT_DAILY_INTERVAL_1 / 2):
                    generate_word_cloud_report(conn, WINDOW_DAILY, "Daily_Run2")
                    last_report_daily_2 = current_time

                # 4. Weekly Report (Last 7 days) - Runs once a day
                if current_time - last_report_weekly >= REPORT_WEEKLY_INTERVAL:
                    generate_word_cloud_report(conn, WINDOW_WEEKLY, "Weekly")
                    last_report_weekly = current_time


                # --- Sleep Logic ---
                sleep_time = min(
                    HARVEST_INTERVAL - (current_time - last_harvest),
                    CHECK_INTERVAL - (current_time - last_check),
                    ANALYSIS_INTERVAL - (current_time - last_analysis),
                    REPORT_HOURLY_INTERVAL - (current_time - last_report_hourly),
                    REPORT_DAILY_INTERVAL_1 - (current_time - last_report_daily_1),
                    REPORT_WEEKLY_INTERVAL - (current_time - last_report_weekly),
                    10 # Cap the sleep time for responsiveness
                )
                
                if sleep_time > 0:
                    time.sleep(sleep_time)

            except praw.exceptions.APIException as e:
                logging.error(f"PRAW API Rate Limit Hit or Error: {e}. Sleeping for 60 seconds.")
                time.sleep(60)
            
            except sqlite3.Error as e:
                logging.error(f"Database Error: {e}. Closing connection and will retry.")
                if conn:
                    conn.close()
                conn = None 
                time.sleep(10) 
            
            except Exception as e:
                logging.critical(f"A critical unhandled error occurred: {e}. Exiting script.")
                break 

    except KeyboardInterrupt:
        logging.info("Script interrupted by user. Shutting down.")
    
    finally:
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
    
    # Load the filter list once
    load_allowed_tickers(TICKER_FILTER_FILE)
    
    # Step 2: Start the continuous loop
    main_loop(reddit)

    print("--- Script has terminated. ---")
