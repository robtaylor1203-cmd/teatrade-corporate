import pandas as pd
import os
import sys
import sqlite3
from datetime import datetime
import logging
import uuid

# =============================================================================
# Configuration (Aligned with TeaTrade Project Structure)
# =============================================================================

# Configure logging to be consistent with your other scripts
logging.basicConfig(level=logging.INFO, format='RA_PROCESSOR: %(levelname)s: %(message)s', handlers=[logging.StreamHandler(sys.stdout)])

# Define the base repository path. 
# IMPORTANT: Ensure this matches the path used in analyze_mombasa.py
REPO_PATH = r"C:\Users\mikin\projects\NewTeaTrade"

# Define database and data directory paths
DB_FILE = os.path.abspath(os.path.join(REPO_PATH, "market_reports.db"))
RA_DATA_DIR = os.path.abspath(os.path.join(REPO_PATH, "RA Certification"))

# Input file definition (UPDATED to match the XLSX filename)
INPUT_FILENAME = 'RACertification_261025.xlsx'
INPUT_FILE_PATH = os.path.join(RA_DATA_DIR, INPUT_FILENAME)

# Define the target table name
TABLE_NAME = 'ra_certification_history'

# =============================================================================
# Database Functions
# =============================================================================

def connect_db():
    """Connects to the SQLite database."""
    logging.info(f"Attempting to connect to database at: {DB_FILE}")
        
    try:
        conn = sqlite3.connect(DB_FILE)
        return conn
    except sqlite3.Error as e:
        logging.error(f"Database connection error: {e}")
        sys.exit(1)

def create_table(conn):
    """Creates the ra_certification_history table if it doesn't exist."""
    # Storing dates as TEXT in YYYY-MM-DD format is standard practice in SQLite.
    schema = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        certificateholder_id TEXT NOT NULL,
        certificateholder_name TEXT,
        crop TEXT,
        license_number TEXT,
        start_date_license TEXT, -- Stored as YYYY-MM-DD
        end_date_license TEXT,   -- Stored as YYYY-MM-DD
        license_standard TEXT,
        license_status TEXT,
        scope TEXT,
        region TEXT,
        country TEXT,
        ingestion_timestamp TEXT NOT NULL, -- Stored as YYYY-MM-DD HH:MM:SS
        batch_id TEXT NOT NULL
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(schema)
        conn.commit()
        logging.info(f"Table '{TABLE_NAME}' checked/created successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error creating table: {e}")
        sys.exit(1)

# =============================================================================
# Data Processing and Cleaning
# =============================================================================

def process_data():
    """Loads (from Excel), cleans the dates, and prepares the data for insertion."""
    
    # Check if the input file exists in the specified directory
    if not os.path.exists(INPUT_FILE_PATH):
        logging.error(f"Error: Input file not found at: {INPUT_FILE_PATH}")
        logging.info(f"Please ensure '{INPUT_FILENAME}' is placed inside the '{RA_DATA_DIR}' folder.")
        sys.exit(1)

    try:
        # Load the Excel file (Changed from pd.read_csv to pd.read_excel)
        logging.info(f"Loading Excel file: {INPUT_FILENAME}")
        # We specify the engine for robustness.
        df = pd.read_excel(INPUT_FILE_PATH, engine='openpyxl')
        
        # Clean column names (Safeguard in case brackets or whitespace exist in the Excel headers)
        original_cols = df.columns
        df.columns = df.columns.str.strip('[]').str.strip()
        if not df.columns.equals(original_cols):
            logging.info("Cleaned characters (brackets/whitespace) from headers.")

        # Identify the date columns
        date_columns = ['start_date_license', 'end_date_license']

        for col in date_columns:
            if col in df.columns:
                logging.info(f"Standardizing date format in column: {col}...")
                # Convert to datetime objects, inferring mixed formats.
                # errors='coerce' turns unparseable dates into NaT (Not a Time).
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Format the dates to the standard SQL format (YYYY-MM-DD)
                # This ensures consistency in the database.
                df[col] = df[col].dt.strftime('%Y-%m-%d')

        # Add tracking columns
        # Use UTC time for standardization
        df['ingestion_timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        # Generate a unique ID for this specific run
        batch_id = str(uuid.uuid4())
        df['batch_id'] = batch_id
        
        logging.info(f"Data processing complete. Records: {len(df)}. Batch ID: {batch_id}")
        return df

    # Catch the error if the required Excel library is missing
    except ImportError:
        logging.error("Error: Pandas requires the 'openpyxl' library to read .xlsx files.")
        logging.error("Please install it using: pip install openpyxl")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An error occurred during data processing: {e}")
        sys.exit(1)

# =============================================================================
# Main Execution
# =============================================================================

def main():
    logging.info("--- Starting RA Certification Data Pipeline ---")

    # 1. Process the data
    data_to_insert = process_data()

    # 2. Connect to the database
    conn = connect_db()
    
    # 3. Ensure the table exists
    create_table(conn)

    # 4. Insert data into the database
    try:
        logging.info(f"Inserting data into {TABLE_NAME} (Append mode)...")
        # Use pandas to_sql for efficient insertion
        # if_exists='append' adds the new data without deleting the old data.
        data_to_insert.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
        logging.info("Data insertion complete.")
    except Exception as e:
        # This will catch issues like column mismatch if the Excel structure changes
        logging.error(f"Error during data insertion (check column matching): {e}")
    finally:
        conn.close()
        logging.info("Database connection closed.")
    
    logging.info("--- Pipeline Finished ---")

if __name__ == '__main__':
    main()