import sqlite3
import pandas as pd
import os
import re
# Use the standard datetime library
from datetime import datetime
import time
import logging
import warnings
import sys
import numpy as np # Import numpy for explicit NaN handling

# Imports for unstructured data processing
try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None
try:
    import docx
except ImportError:
    docx = None

# =============================================================================
# Configuration (V23 - Synchronized with V15 Pipeline)
# =============================================================================

# Force logging to stdout
# V23: Updated logging prefix
logging.basicConfig(level=logging.INFO, format='PROCESSOR V23: %(levelname)s: %(message)s', handlers=[logging.StreamHandler(sys.stdout)])

# Dynamically determine the repository path based on the script's location.
try:
    # __file__ is the path to the current script. dirname() gets the directory containing it.
    REPO_PATH = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Fallback for environments where __file__ might not be defined (e.g., interactive interpreter)
    REPO_PATH = os.path.abspath(os.getcwd())

logging.info(f"[ENVIRONMENT_DIAGNOSTIC] Determined REPO_PATH as: {REPO_PATH}")


# Define DB_FILE and MOMBASA_DIR using the dynamic REPO_PATH
DB_FILE = os.path.abspath(os.path.join(REPO_PATH, "market_reports.db"))
MOMBASA_DIR = os.path.abspath(os.path.join(REPO_PATH, "Mombasa"))

SOURCE_LOCATION = "Mombasa"

warnings.filterwarnings("ignore", message="Cannot parse header or footer so it will be ignored")


# Define Data Types
DATA_TYPE_OFFER = 'OFFER'
DATA_TYPE_SALE = 'SALE'
DATA_TYPE_SUMMARY = 'SUMMARY'
DATA_TYPE_COMMENTARY = 'COMMENTARY'

# Define noise values centrally for cleaning
NOISE_VALUES = {'NAN', 'NONE', '', '-', 'NIL', 'N/A', 'NULL', 'UNKNOWN'}

# Flexible Column Mapping (including 'Bags' and 'Pkgs')
MARK_ALIASES = ['Selling Mark', 'Garden', 'Mark', 'Estate', 'Factory', 'Selling Mark - MF Mark']
COLUMN_MAP_LOT_DETAILS = {
    'broker': ['Broker'],
    'mark': MARK_ALIASES,
    'grade': ['Grade'],
    'lot_number': ['LotNo', 'Lot No', 'Lot', 'Lot.No'],
    'invoice_number': ['Invoice', 'Inv.No', 'Invoice No'],
    'quantity_kgs': ['Net Weight', 'Kilos', 'Kgs', 'Quantity (Kg)', 'Total Weight', 'Net Kgs', 'Weight'],
    'package_count': ['Bags', 'Pkgs', 'Packages', 'Package', 'Pks', 'No of Pkgs', 'Units', 'Count'],
    'price': ['Purchased Price', 'Final Price', 'Price', 'Price (USD)', 'Price (USc)'],
    'valuation_or_rp': ['Valuation', 'Asking Price', 'RP'],
    'buyer': ['Buyer', 'Buyer Name', 'Final Buyer'],
    'sale_date_internal': ['Selling End Time', 'Sale Date'],
    'sale_number_internal': ['Sale Code', 'Auction'],
}

COLUMN_MAP_GRADE_SUMMARY = {
    'grade': ['Region/Grade'],
    'lots': ['Lots'],
    'quantity_kgs': ['Kilos', 'Pkgs', 'Kgs'],
}

# Keywords used for dynamic header detection
HEADER_KEYWORDS = ['LotNo', 'Garden', 'Grade', 'Invoice', 'Pkgs', 'Kilos', 'RP', 'Valuation', 'Price', 'Buyer', 'Mark', 'Lot', 'Broker', 'Weight', 'Bags']

# =============================================================================
# Database Initialization (V18 Schema)
# =============================================================================

def initialize_database():
    logging.info(f"[DB_DIAGNOSTIC] Initializing database at absolute path: {DB_FILE}")
    db_dir = os.path.dirname(DB_FILE)
    if not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
        except OSError as e:
            logging.error(f"[DB_DIAGNOSTIC] Directory creation failed: {e}")
            return False

    try:
        with sqlite3.connect(DB_FILE) as conn:
            # Create tables using the latest V18 schema
            # (Table creation logic remains the same as previous stable versions)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processing_log (
                    id INTEGER PRIMARY KEY, file_identifier TEXT NOT NULL, processed_timestamp TEXT NOT NULL,
                    records_inserted INTEGER, data_type TEXT NOT NULL, status TEXT NOT NULL,
                    UNIQUE(file_identifier, data_type)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS auction_sales (
                    id INTEGER PRIMARY KEY, source_location TEXT NOT NULL, sale_date TEXT, sale_number TEXT,
                    broker TEXT, mark TEXT, grade TEXT, lot_number TEXT NOT NULL, invoice_number TEXT,
                    quantity_kgs REAL, package_count INTEGER, price REAL, buyer TEXT,
                    source_file_identifier TEXT NOT NULL, processed_timestamp TEXT NOT NULL,
                    UNIQUE(source_location, sale_number, lot_number)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS auction_offers (
                    id INTEGER PRIMARY KEY, source_location TEXT NOT NULL, sale_date TEXT, sale_number TEXT,
                    broker TEXT, mark TEXT, grade TEXT, lot_number TEXT NOT NULL, invoice_number TEXT,
                    quantity_kgs REAL, package_count INTEGER, valuation_or_rp REAL,
                    source_file_identifier TEXT NOT NULL, processed_timestamp TEXT NOT NULL,
                    UNIQUE(source_location, sale_number, lot_number)
                )
            """)
            conn.execute("""
                 CREATE TABLE IF NOT EXISTS grade_summary (
                    id INTEGER PRIMARY KEY, source_location TEXT NOT NULL, sale_date TEXT, sale_number TEXT,
                    auction_type TEXT NOT NULL, grade TEXT NOT NULL, lots INTEGER, quantity_kgs REAL,
                    source_file_identifier TEXT NOT NULL, processed_timestamp TEXT NOT NULL,
                    UNIQUE(source_location, sale_number, auction_type, grade)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS market_commentary (
                    id INTEGER PRIMARY KEY,
                    source_location TEXT NOT NULL,
                    report_date TEXT,
                    sale_number TEXT,
                    content_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    processed_timestamp TEXT NOT NULL,
                    UNIQUE(source_location, sale_number, content_type, source_file)
                )
            """)
            conn.commit()
            logging.info("[DB_DIAGNOSTIC] Database schema initialized/verified successfully.")

        # DIAGNOSTIC: Verify file existence
        if os.path.exists(DB_FILE):
            return True
        else:
            logging.error("[DB_DIAGNOSTIC] Database file was NOT created.")
            return False

    except sqlite3.Error as e:
        logging.error(f"Database initialization error: {e}")
        return False

# =============================================================================
# Utility Functions (Logging, Mapping, Parsing, Cleaning)
# =============================================================================

# (Utility functions remain the same as previous stable version V22)
def clean_and_cast_numeric_columns(df):
    """
    Ensures numeric columns are rigorously cleaned.
    Reverts integer columns (package_count, lots) to float64 for maximum SQLite compatibility (BLOB fix).
    """
    logging.info("  [CLEANING] Applying rigorous cleaning and casting (Float64 Compatibility Mode)...")

    # Define columns
    float_cols = ['quantity_kgs', 'price', 'valuation_or_rp']
    integer_like_cols = ['package_count', 'lots']

    # Helper to clean up strings before conversion
    def clean_col(series, allow_decimal=True):
        # Ensure data is treated as a string for cleaning operations
        series = series.astype(str)
        # Remove common noise: currency symbols, thousands separators (commas)
        series = series.str.replace(r'[$,]', '', regex=True)

        # If decimals are not allowed, remove everything after the decimal point
        if not allow_decimal:
            series = series.str.split('.').str[0]

        # Replace empty strings or strings that became empty after cleaning with NaN
        series = series.replace(r'^\s*$', np.nan, regex=True)
        return series

    # Process Float columns
    for col in float_cols:
        if col in df.columns:
            df[col] = clean_col(df[col], allow_decimal=True)
            # Coerce to numeric, setting unparseable values to NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].astype('float64')

    # Process Integer-like columns (Treated as Float64)
    for col in integer_like_cols:
        if col in df.columns:
            df[col] = clean_col(df[col], allow_decimal=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # Use float64 instead of Int64 for BLOB compatibility
            df[col] = df[col].astype('float64')

    logging.info("  [CLEANING] Rigorous cleaning and casting complete.")
    return df

def log_processing_status(conn, file_identifier, data_type, records_inserted, status):
    try:
        timestamp = datetime.now().isoformat()
        # Use INSERT OR REPLACE to handle updates if rerunning the same file/type combo
        conn.execute("""
            INSERT OR REPLACE INTO processing_log
            (file_identifier, processed_timestamp, records_inserted, data_type, status)
            VALUES (?, ?, ?, ?, ?)
        """, (file_identifier, timestamp, records_inserted, data_type, status))
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to log processing status for {file_identifier}: {e}")

def check_already_processed(conn, file_identifier, data_type):
    try:
        cursor = conn.execute("""
            SELECT status FROM processing_log WHERE file_identifier = ? AND data_type = ?
        """, (file_identifier, data_type))
        result = cursor.fetchone()
        return result is not None and result[0] == 'SUCCESS'
    except sqlite3.Error as e:
        logging.error(f"Database error checking processing status: {e}")
        return False

def map_columns(df, column_map):
    """Maps detected column names to standardized internal names."""
    rename_dict = {}

    for col in df.columns:
        # Normalize the column name from the dataframe
        normalized_col = str(col).strip().upper()

        # Iterate through the standardized names and their aliases
        for standardized_name, aliases in column_map.items():
            # Normalize the aliases for comparison
            normalized_aliases = [str(alias).strip().upper() for alias in aliases]
            if normalized_col in normalized_aliases:
                # If a match is found, add it to the rename dictionary
                if standardized_name not in rename_dict.values():
                     rename_dict[col] = standardized_name
                break

    # Apply the renaming
    df_mapped = df.rename(columns=rename_dict)

    # Ensure all required standardized columns exist, even if empty
    for standardized_name in column_map.keys():
        if standardized_name not in df_mapped.columns:
            df_mapped[standardized_name] = np.nan # Use numpy NaN for consistency

    # Keep only the columns defined in the map
    df_final = df_mapped[list(column_map.keys())]

    return df_final

def parse_date(date_str):
    """Robustly parses various date formats into ISO 8601 (YYYY-MM-DD)."""
    if pd.isna(date_str) or str(date_str).strip().upper() in NOISE_VALUES:
        return None

    # Handle potential numeric (Excel serial date) inputs first
    if isinstance(date_str, (int, float)):
        try:
            # Excel epoch adjustment (Using 1899-12-30 handles the Excel 1900 leap year bug if required)
            return (datetime(1899, 12, 30) + pd.to_timedelta(date_str, unit='D')).strftime('%Y-%m-%d')
        except Exception:
            pass # Fall through to string parsing

    date_str = str(date_str)

    # Common date formats observed in the reports
    formats = [
        '%d/%m/%Y %H:%M:%S:%f', # Format seen in GeneralReport (e.g., 02/09/2025 12:49:12:300)
        '%Y-%m-%d %H:%M:%S',
        '%Y/%m/%d',          # Year first (e.g., 2025/07/29)
        '%d-%b-%Y',
        '%d/%m/%Y',
        '%m/%d/%Y',
        '%d.%m.%Y',
    ]

    for fmt in formats:
        try:
            # Parse and reformat to ISO 8601
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue

    # If all formats fail, try pandas generic parser
    try:
        return pd.to_datetime(date_str).strftime('%Y-%m-%d')
    except Exception:
        logging.warning(f"Could not parse date: {date_str}")
        return None

def clean_text_columns(df):
    """Strips whitespace and converts text columns to uppercase."""
    text_cols = ['broker', 'mark', 'grade', 'lot_number', 'invoice_number', 'buyer', 'sale_number', 'sale_date']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
            # Replace 'NAN' (from astype(str)) and other noise with actual NaN
            df[col] = df[col].replace(NOISE_VALUES, np.nan)
    return df

# =============================================================================
# File Reading Functions (GeneralReport Fix Integrated)
# =============================================================================

# (File reading functions remain the same as previous stable version V22)
def find_header_row(df, keywords, max_rows=15):
    """
    Dynamically finds the header row by looking for the row with the maximum keyword matches.
    (Used for non-GeneralReport files).
    """
    # Ensure keywords are normalized
    normalized_keywords = set([str(k).strip().upper() for k in keywords])

    best_match_count = 0
    header_row_index = 0 # Default to the first row

    # Limit the search to the first 'max_rows'
    search_df = df.head(max_rows)

    for index, row in search_df.iterrows():
        # Normalize the row values
        row_values = set([str(v).strip().upper() for v in row if pd.notna(v)])

        # Count matches
        match_count = len(normalized_keywords.intersection(row_values))

        if match_count > best_match_count:
            best_match_count = match_count
            header_row_index = index

    # Threshold check
    if best_match_count < min(4, len(keywords)):
        logging.warning(f"  [HEADER_DETECTION] Header row not confidently detected (Matches: {best_match_count}). Defaulting to row 0.")
        return 0

    logging.info(f"  [HEADER_DETECTION] Header row detected at index {header_row_index} (Matches: {best_match_count}).")
    return header_row_index


def read_csv_file(filepath, keywords):
    """Reads a CSV file, applying specific logic for GeneralReport or dynamic detection for others."""
    try:
        # Check if it's a GeneralReport file (case-insensitive check)
        if "GENERALREPORT" in os.path.basename(filepath).upper():
            logging.info("  [FILE_READ] Detected 'GeneralReport' (CSV). Applying specific parsing logic (Header=0, Skiprows=[1]).")
            # Use the first row (index 0) as the header AND skip the second row (index 1).
            df = pd.read_csv(filepath, header=0, skiprows=[1], low_memory=False)
            return df

        # Standard logic for other files
        # Step 1: Read the first few rows to detect the header
        temp_df = pd.read_csv(filepath, header=None, nrows=20, low_memory=False)
        if temp_df.empty: return pd.DataFrame()

        header_row = find_header_row(temp_df, keywords)

        # Step 2: Read the full file using the detected header row
        df = pd.read_csv(filepath, header=header_row, low_memory=False)
        return df

    except Exception as e:
        logging.error(f"Error reading CSV file {filepath}: {e}")
        return pd.DataFrame()

def read_excel_file(filepath, keywords):
    """Reads an Excel file, applying specific logic for GeneralReport or dynamic detection for others."""
    try:
        # Check if it's a GeneralReport file (case-insensitive check)
        if "GENERALREPORT" in os.path.basename(filepath).upper():
            logging.info("  [FILE_READ] Detected 'GeneralReport' (Excel). Applying specific parsing logic (Header=0, Skiprows=[1]).")
            # Use the first row (index 0) as the header AND skip the second row (index 1).
            # Assuming data is on the first sheet for GeneralReport Excel files.
            # Using openpyxl for better compatibility with modern Excel formats.
            df = pd.read_excel(filepath, header=0, skiprows=[1], engine='openpyxl')
            return df

        # Standard logic for other Excel files (handling multiple sheets)
        # Try openpyxl first, fallback if needed
        try:
            engine = 'openpyxl'
            xls = pd.ExcelFile(filepath, engine=engine)
        except Exception as e:
             logging.warning(f"Failed to read Excel with openpyxl, trying default engine. Error: {e}")
             engine = None
             xls = pd.ExcelFile(filepath, engine=engine)

        all_sheets_df = []

        for sheet_name in xls.sheet_names:
            # Step 1: Read the first few rows of the sheet to detect the header
            temp_df = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=20)
            if temp_df.empty: continue

            header_row = find_header_row(temp_df, keywords)

            # Step 2: Read the sheet using the detected header row
            df = pd.read_excel(xls, sheet_name=sheet_name, header=header_row)
            all_sheets_df.append(df)

        if not all_sheets_df: return pd.DataFrame()

        # Concatenate all sheets if multiple are found
        return pd.concat(all_sheets_df, ignore_index=True)

    except Exception as e:
        logging.error(f"Error reading Excel file {filepath}: {e}")
        return pd.DataFrame()

def read_file(filepath, keywords):
    """General function to read data based on file extension."""
    logging.info(f"  [FILE_READ] Attempting to read file: {os.path.basename(filepath)}")
    start_time = time.time()

    ext = os.path.splitext(filepath)[1].lower()

    if ext == '.csv':
        df = read_csv_file(filepath, keywords)
    elif ext in ['.xls', '.xlsx', '.xlsm']:
        df = read_excel_file(filepath, keywords)
    else:
        logging.warning(f"Unsupported file format: {ext}")
        return pd.DataFrame()

    # General cleanup
    if not df.empty:
        df.dropna(how='all', inplace=True)
        df.dropna(axis=1, how='all', inplace=True)

    end_time = time.time()
    logging.info(f"  [FILE_READ] Finished reading file in {end_time - start_time:.2f} seconds. Initial rows: {len(df)}")
    return df

# =============================================================================
# Metadata Extraction Functions
# =============================================================================

# (Metadata functions remain the same as previous stable version V22)
def extract_metadata_from_filename(filename):
    """
    Extracts Sale Number and Date from the filename using regex.
    """
    sale_number = None
    sale_date = None

    # Pattern 1: Mombasa_Sale_No_XX_Date_Year (Handles complex dates)
    pattern1 = re.search(r"Sale_No_(\d+)_", filename, re.IGNORECASE)
    if pattern1:
        sale_number = pattern1.group(1)
        # Attempt to extract complex date part (e.g., 14th_15th_October_2025)
        date_part_match = re.search(r"(\d{1,2}(st|nd|rd|th)?_.*?_\d{4})", filename, re.IGNORECASE)
        if date_part_match:
             date_str = date_part_match.group(1).replace('_', ' ')
             try:
                 # Rely on parse_date being robust
                 sale_date = parse_date(date_str)
             except:
                 pass

    # Pattern 2: Sale XX (Date)
    pattern2 = re.search(r"Sale\s*(\d+)\s*\((.*?)\)", filename, re.IGNORECASE)
    if pattern2 and not sale_number:
        sale_number = pattern2.group(1)
        if not sale_date:
            sale_date = parse_date(pattern2.group(2))

    # Pattern 3: GeneralReport (XX)
    pattern3 = re.search(r"GeneralReport\s*\((\d+)\)", filename, re.IGNORECASE)
    if pattern3 and not sale_number:
         # The number in parenthesis might be a report ID rather than sale number, but we capture it if nothing else is found.
         sale_number = pattern3.group(1)
         # Date extraction not possible from this format alone

    return sale_number, sale_date

def extract_metadata_from_dataframe(df):
    """
    Extracts Sale Number and Date from internal columns if available.
    Uses 'sale_number_internal' and 'sale_date_internal' (mapped columns).
    """
    sale_number = None
    sale_date = None

    # Extract Sale Number (e.g., "Sale 35 - M2")
    if 'sale_number_internal' in df.columns and df['sale_number_internal'].notna().any():
        try:
            # Use the mode (most frequent value)
            raw_sale_code = df['sale_number_internal'].dropna().mode()[0]
            match = re.search(r"Sale\s*(\d+)", str(raw_sale_code), re.IGNORECASE)
            if match:
                sale_number = match.group(1)
        except Exception as e:
            logging.warning(f"Could not extract internal sale number: {e}")

    # Extract Sale Date
    if 'sale_date_internal' in df.columns and df['sale_date_internal'].notna().any():
        try:
            raw_date = df['sale_date_internal'].dropna().mode()[0]
            sale_date = parse_date(raw_date)
        except Exception as e:
             logging.warning(f"Could not extract internal sale date: {e}")

    return sale_number, sale_date

def determine_final_metadata(filename, df):
    """
    Combines metadata extraction from filename and dataframe, prioritizing internal data.
    """
    fn_sale_number, fn_sale_date = extract_metadata_from_filename(filename)
    df_sale_number, df_sale_date = extract_metadata_from_dataframe(df)

    # Prioritize internal data (from the dataframe) as it's often more accurate
    final_sale_number = df_sale_number if df_sale_number else fn_sale_number
    final_sale_date = df_sale_date if df_sale_date else fn_sale_date

    if not final_sale_number:
        logging.warning(f"  [METADATA] Sale number could not be determined for {filename}.")
    if not final_sale_date:
        logging.warning(f"  [METADATA] Sale date could not be determined for {filename}.")

    return final_sale_number, final_sale_date

# =============================================================================
# Data Processing and Insertion Functions
# =============================================================================

# (Processing and Insertion functions remain the same as previous stable version V22)
def process_lot_details(df, metadata):
    """Processes the main lot details (Offers and Sales)."""
    logging.info("  [PROCESSING] Starting lot details processing...")

    # 1. Mapping
    df_mapped = map_columns(df, COLUMN_MAP_LOT_DETAILS)

    # 2. Determine Metadata (needs the mapped dataframe for internal columns)
    sale_number, sale_date = determine_final_metadata(metadata['filename'], df_mapped)

    # 3. Cleaning and Casting
    df_cleaned = clean_text_columns(df_mapped)
    df_final = clean_and_cast_numeric_columns(df_cleaned)

    # 4. Add Metadata Columns
    df_final['source_location'] = SOURCE_LOCATION
    df_final['sale_number'] = sale_number
    df_final['sale_date'] = sale_date
    df_final['source_file_identifier'] = metadata['file_identifier']
    df_final['processed_timestamp'] = metadata['timestamp']

    # 5. Validation
    essential_cols = ['lot_number', 'mark', 'grade', 'quantity_kgs']
    df_final = df_final.dropna(subset=essential_cols, how='all')
    # Ensure lot_number is not null specifically, as it's a primary component of the unique key
    df_final = df_final[df_final['lot_number'].notna()]


    if df_final.empty:
        logging.warning("  [PROCESSING] No valid lot details found after cleaning.")
        return pd.DataFrame(), pd.DataFrame()

    # 6. Split into Offers and Sales
    # Identify Sales: Must have a price. (This correctly handles GeneralReport where unsold lots have no price)
    sales_df = df_final[df_final['price'].notna()].copy()

    # Identify Offers: Includes everything listed (the catalogue).
    offers_df = df_final.copy()

    # 7. Final Column Selection
    sales_cols_to_keep = ['source_location', 'sale_date', 'sale_number', 'broker', 'mark', 'grade',
                          'lot_number', 'invoice_number', 'quantity_kgs', 'package_count', 'price', 'buyer',
                          'source_file_identifier', 'processed_timestamp']
    offers_cols_to_keep = ['source_location', 'sale_date', 'sale_number', 'broker', 'mark', 'grade',
                           'lot_number', 'invoice_number', 'quantity_kgs', 'package_count', 'valuation_or_rp',
                           'source_file_identifier', 'processed_timestamp']

    sales_df = sales_df[[col for col in sales_cols_to_keep if col in sales_df.columns]]
    offers_df = offers_df[[col for col in offers_cols_to_keep if col in offers_df.columns]]

    logging.info(f"  [PROCESSING] Lot details finalized. Offers: {len(offers_df)}, Sales: {len(sales_df)}")
    return offers_df, sales_df

def process_grade_summary(df, metadata, auction_type):
    """Processes grade summary data (Placeholder)."""
    logging.info(f"  [PROCESSING] Grade summary processing ({auction_type}) - Placeholder.")
    # Implementation for summary data would go here if needed.
    return pd.DataFrame()


def insert_data(conn, df, table_name):
    """
    Inserts dataframe into the specified SQLite table.
    Includes robust fallback for IntegrityErrors during bulk insert.
    """
    if df.empty:
        return 0

    try:
        # Attempt bulk insert first (faster)
        # We use a slightly safer way to count records using the cursor
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        records_before = cursor.fetchone()[0]
        
        df.to_sql(table_name, conn, if_exists='append', index=False)
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        records_after = cursor.fetchone()[0]

        inserted_count = records_after - records_before
        return inserted_count

    except sqlite3.IntegrityError:
        logging.warning(f"  [DB_INSERT] Integrity error during bulk insert into {table_name}. Falling back to row-by-row insertion to skip duplicates.")

        # Fallback to row-by-row insertion (slower but robust against duplicates)
        inserted_count = 0
        for index, row in df.iterrows():
            try:
                # Convert the row Series to a single-row DataFrame for insertion
                row.to_frame().T.to_sql(table_name, conn, if_exists='append', index=False)
                inserted_count += 1
            except sqlite3.IntegrityError:
                # Ignore the duplicate row and continue
                continue
            except Exception as e_row:
                logging.error(f"  [DB_INSERT_ERROR] Failed to insert row {index} into {table_name}: {e_row}")
        return inserted_count

    except Exception as e:
        logging.error(f"  [DB_INSERT_ERROR] Failed to insert data into {table_name}: {e}")
        return 0

# =============================================================================
# Main Orchestration Functions
# =============================================================================

def process_structured_data(filepath, data_type, conn):
    """Orchestrates the reading, processing, and insertion of structured data."""
    logging.info(f"[ORCHESTRATOR] Processing structured file: {os.path.basename(filepath)} as Type: {data_type}")

    # Use file modification time + name as a unique identifier
    try:
        file_mod_time = os.path.getmtime(filepath)
        file_identifier = f"{os.path.basename(filepath)}|{file_mod_time}"
    except OSError as e:
        logging.error(f"Could not access file stats for {filepath}: {e}")
        return

    # Check if already processed successfully
    if check_already_processed(conn, file_identifier, data_type):
        logging.info(f"[ORCHESTRATOR] File already processed successfully. Skipping.")
        return

    metadata = {
        'file_identifier': file_identifier,
        'filename': os.path.basename(filepath),
        'timestamp': datetime.now().isoformat()
    }

    # Determine keywords (used only if dynamic detection is needed for non-GeneralReport files)
    if data_type in [DATA_TYPE_OFFER, DATA_TYPE_SALE]:
        keywords = HEADER_KEYWORDS
    elif data_type == DATA_TYPE_SUMMARY:
        keywords = list(COLUMN_MAP_GRADE_SUMMARY.keys())
    else:
        return

    # Read the file (V21 Fix applied within this function)
    df = read_file(filepath, keywords)

    if df.empty:
        logging.warning("[ORCHESTRATOR] File is empty or could not be read. Logging as FAILURE.")
        log_processing_status(conn, file_identifier, data_type, 0, 'FAILURE')
        return

    # Process and Insert
    total_inserted = 0
    try:
        if data_type in [DATA_TYPE_OFFER, DATA_TYPE_SALE]:
            offers_df, sales_df = process_lot_details(df, metadata)

            if not offers_df.empty:
                 inserted = insert_data(conn, offers_df, 'auction_offers')
                 logging.info(f"[ORCHESTRATOR] Inserted {inserted} new offer records.")
                 total_inserted += inserted

            if not sales_df.empty:
                inserted = insert_data(conn, sales_df, 'auction_sales')
                logging.info(f"[ORCHESTRATOR] Inserted {inserted} new sale records.")
                total_inserted += inserted

        elif data_type == DATA_TYPE_SUMMARY:
             # ... Summary logic ...
             pass

        # Log final status
        status = 'SUCCESS' if total_inserted > 0 else 'NO_NEW_DATA'
        log_processing_status(conn, file_identifier, data_type, total_inserted, status)
        logging.info(f"[ORCHESTRATOR] Finished processing. Status: {status}")

    except Exception as e:
        logging.error(f"[ORCHESTRATOR] An error occurred during processing of {filepath}: {e}", exc_info=True)
        log_processing_status(conn, file_identifier, data_type, total_inserted, 'FAILURE')


def identify_file_type(filename):
    """Identifies the data type based on filename patterns."""
    name = filename.lower()

    # GeneralReport contains both offers and sales data (treated as SALE type for unified processing)
    if "generalreport" in name:
        return DATA_TYPE_SALE, 'structured'

    if "offer" in name or "catalogue" in name:
        return DATA_TYPE_OFFER, 'structured'

    if "sale" in name or "result" in name or "price list" in name:
        if "summary" in name or "average" in name:
             return DATA_TYPE_SUMMARY, 'structured'
        else:
            return DATA_TYPE_SALE, 'structured'

    if "commentary" in name or "market report" in name:
        return DATA_TYPE_COMMENTARY, 'unstructured'

    # Default/Fallback
    ext = os.path.splitext(filename)[1].lower()
    if ext in ['.csv', '.xls', '.xlsx']:
        logging.info(f"  [IDENTIFY] File type ambiguous. Defaulting to SALE for structured file: {filename}")
        return DATA_TYPE_SALE, 'structured'

    return None, None

def main():
    """Main execution loop."""
    logging.info("--- Starting Mombasa Data Processor V23 ---")
    
    # Log the detected path
    logging.info(f"[PATH_DIAGNOSTIC] Target MOMBASA_DIR: {MOMBASA_DIR}")

    if not initialize_database():
        logging.error("Database initialization failed. Exiting.")
        sys.exit(1)

    try:
        conn = sqlite3.connect(DB_FILE)
    except sqlite3.Error as e:
        logging.error(f"Failed to connect to the database: {e}. Exiting.")
        sys.exit(1)


    try:
        if not os.path.exists(MOMBASA_DIR):
            # Provide clearer error message for CI/CD debugging
            logging.error(f"CRITICAL ERROR: Mombasa directory not found at the expected location: {MOMBASA_DIR}.")
            logging.error("Ensure the repository is checked out correctly and the directory structure is intact. Exiting.")
            sys.exit(1)

        logging.info(f"Scanning directory: {MOMBASA_DIR}")
        processed_files = 0
        # Iterating through all files in the directory
        for filename in os.listdir(MOMBASA_DIR):
            filepath = os.path.join(MOMBASA_DIR, filename)

            if os.path.isdir(filepath):
                continue

            # Skip temporary/hidden files
            if filename.startswith('~') or filename.startswith('.'):
                continue

            data_type, structure_type = identify_file_type(filename)

            if data_type:
                logging.info(f"\n--- Processing File: {filename} (Type: {data_type}, Structure: {structure_type}) ---")
                if structure_type == 'structured':
                    process_structured_data(filepath, data_type, conn)
                elif structure_type == 'unstructured':
                    # process_unstructured_data(filepath, data_type, conn)
                    pass # Placeholder
                processed_files += 1
            else:
                logging.info(f"Skipping unrecognized file: {filename}")

        if processed_files == 0:
            logging.info("No processable files found in the directory.")

    except Exception as e:
        logging.error(f"An unexpected error occurred during the main loop: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
        logging.info("--- Mombasa Data Processor Finished ---")

if __name__ == "__main__":
    main()