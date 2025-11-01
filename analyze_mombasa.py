import sqlite3
import pandas as pd
import logging
import os
import sys
# Use the standard datetime library
import datetime
import altair as alt
import json
import numpy as np

# =============================================================================
# Configuration (V12 - Absolute Paths)
# =============================================================================

# Define the base repository path
# IMPORTANT: Ensure this path is correct for your local setup.
REPO_PATH = r"C:\Users\mikin\projects\NewTeaTrade"

# V12: Define DB_FILE using an absolute path to avoid ambiguity
DB_FILE = os.path.abspath(os.path.join(REPO_PATH, "market_reports.db"))

# V12: Define output directories using absolute paths
DATA_OUTPUT_DIR = os.path.abspath(os.path.join(REPO_PATH, "report_data"))
INDEX_FILE = os.path.join(DATA_OUTPUT_DIR, "mombasa_index.json")


PRIMARY_COLOR = "#4285F4" # Google Blue
LIGHTER_BLUE = "#a6c8ff"  # Lighter Blue (for inactive elements)
HISTORICAL_TICK_COLOR = "#000000" # Black for historical markers
CHART_HEIGHT = 320
PLACEHOLDER = "N/A (Pending)"

# Define centralized data source names
DATA_SOURCE_WEEK = 'source_sales_week'
DATA_SOURCE_PREV_GRADE = 'source_prev_grade_metrics'
DATA_SOURCE_PREV_BROKER = 'source_prev_broker_metrics'
DATA_SOURCE_MOVEMENT = 'source_movement_data'


# Configure logging
# V12: Force logging to stdout
logging.basicConfig(level=logging.INFO, format='ANALYZER: %(levelname)s: %(message)s', handlers=[logging.StreamHandler(sys.stdout)])

NOISE_VALUES = {'NAN', 'NONE', '', '-', 'NIL', 'N/A', 'NULL', 'UNKNOWN'}
alt.data_transformers.disable_max_rows()

# =============================================================================
# Helper Functions (Database, Cleaning, and Data Prep)
# =============================================================================

def connect_db():
    # V12 DIAGNOSTIC: Confirm which DB is being accessed
    logging.info(f"[DB_DIAGNOSTIC] Attempting to connect to database at: {DB_FILE}")
    
    if not os.path.exists(DB_FILE):
        logging.warning(f"[DB_DIAGNOSTIC] Database file not found: {DB_FILE}. Analysis cannot proceed.");
        return None 
    try: return sqlite3.connect(DB_FILE)
    except sqlite3.Error as e:
        logging.error(f"Database connection error: {e}"); sys.exit(1)

def clean_text_column(df, column_name):
    if column_name in df.columns:
        df[column_name] = df[column_name].astype(str).str.strip().str.upper()
        df[column_name] = df[column_name].replace(NOISE_VALUES, pd.NA)
    return df

def get_previous_week_df(sales_df_all, current_sale_number):
    if sales_df_all.empty or current_sale_number is None:
        return pd.DataFrame()
    try:
        # Ensure comparison is robust by casting both to string for safety
        current_sale_str = str(current_sale_number)
        previous_sales = sales_df_all[sales_df_all['sale_number'].astype(str) < current_sale_str]
    except Exception as e:
        logging.warning(f"Error during previous week comparison: {e}")
        return pd.DataFrame()
        
    if not previous_sales.empty:
        # Find the max sale number among the previous sales
        previous_sale_number = previous_sales['sale_number'].max()
        return sales_df_all[sales_df_all['sale_number'] == previous_sale_number]
    return pd.DataFrame()

# *** UPDATED FUNCTION: Includes BLOB/Bytes decoding ***
def fetch_data(conn):
    if conn is None: return pd.DataFrame(), pd.DataFrame()
    try:
        # Check table existence
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='auction_sales';")
        sales_exists = cursor.fetchone() is not None
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='auction_offers';")
        offers_exists = cursor.fetchone() is not None

        if not sales_exists and not offers_exists:
             logging.warning("Essential tables not found. Returning empty dataframes.")
             return pd.DataFrame(), pd.DataFrame()

        sales_df = pd.read_sql_query("SELECT * FROM auction_sales", conn) if sales_exists else pd.DataFrame()
        offers_df = pd.read_sql_query("SELECT * FROM auction_offers", conn) if offers_exists else pd.DataFrame()

        # FIX: Robust numeric conversion handling potential BLOBs (bytes)
        def robust_to_numeric(series):
            # Check if the series contains bytes (often indicates BLOB storage)
            if series.dtype == 'object' and series.apply(lambda x: isinstance(x, bytes)).any():
                logging.info(f"[BLOB_FIX] Detected bytes (BLOB) in column '{series.name}'. Attempting to decode.")
                try:
                    # Decode bytes to string (e.g., b'40' -> '40'). Use errors='ignore' for safety.
                    series = series.apply(lambda x: x.decode('utf-8', errors='ignore') if isinstance(x, bytes) else x)
                except Exception as e:
                    logging.error(f"[BLOB_FIX] Failed during decoding process for '{series.name}': {e}")
            # Convert the resulting data (now strings or numbers) to numeric
            return pd.to_numeric(series, errors='coerce')

        sales_cols = ['price', 'quantity_kgs', 'package_count']
        offers_cols = ['valuation_or_rp', 'quantity_kgs', 'package_count']

        # CRITICAL: Ensure columns are numeric immediately upon fetching from DB.
        for df, cols in [(sales_df, sales_cols), (offers_df, offers_cols)]:
            for col in cols:
                # Use the robust converter instead of the basic pd.to_numeric
                if col in df.columns:
                    # Apply the fix here
                    df[col] = robust_to_numeric(df[col])

        text_cols_common = ['mark', 'grade', 'broker', 'lot_number', 'sale_number', 'sale_date']
        for df in [sales_df, offers_df]:
            for col in text_cols_common: df = clean_text_column(df, col)

        if 'buyer' in sales_df.columns: sales_df = clean_text_column(sales_df, 'buyer')

        # Basic validation of keys
        keys = ['broker', 'lot_number', 'sale_number', 'sale_date']
        if not sales_df.empty and all(k in sales_df.columns for k in keys):
            sales_df = sales_df.dropna(subset=keys)
            if 'sale_number' in sales_df.columns: sales_df = sales_df[sales_df['sale_number'].notna()]

        if not offers_df.empty and all(k in offers_df.columns for k in keys):
            offers_df = offers_df.dropna(subset=keys)
            if 'sale_number' in offers_df.columns: offers_df = offers_df[offers_df['sale_number'].notna()]

        # Ensure sale_number is treated as a string consistently
        if 'sale_number' in sales_df.columns: sales_df['sale_number'] = sales_df['sale_number'].astype(str)
        if 'sale_number' in offers_df.columns: offers_df['sale_number'] = offers_df['sale_number'].astype(str)

        return sales_df, offers_df
    except Exception as e:
        logging.error(f"Error fetching data: {e}", exc_info=True); sys.exit(1)

# Centralized Robust Total Weight Calculation (KGs * Packages) with Diagnostics
# NOTE: This function's logic was already robust; it now benefits from the clean data provided by the updated fetch_data.
def calculate_total_weight(df_raw, data_type="Data"):
    """
    Calculates total_weight_kgs = quantity_kgs * package_count.
    Ensures package_count defaults to 1 if missing, NaN, zero, or invalid.
    Includes diagnostics.
    """
    logging.info(f"Calculating Total Weight for {data_type}...")
    
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()

    # 1. Check essential 'quantity_kgs' (KGs per package)
    if 'quantity_kgs' not in df.columns:
        logging.warning(f"  [DIAGNOSTIC] 'quantity_kgs' missing in {data_type}. Cannot calculate total weight.")
        df['total_weight_kgs'] = 0
        return df
    
    # Ensure quantity_kgs is numeric (fetch_data should have done this, but defensive check)
    df['quantity_kgs'] = pd.to_numeric(df['quantity_kgs'], errors='coerce')


    # 2. Ensure 'package_count' column exists. If not in DB schema, create it.
    if 'package_count' not in df.columns:
        # V12: This warning is now critical, as migration should ensure the column exists.
        logging.warning(f"  [DIAGNOSTIC] 'package_count' column missing from DB schema for {data_type}. Defaulting to 1 package per lot. Check Migration logs in Processor.")
        df['package_count'] = pd.NA # Initialize column if missing
    else:
        logging.info(f"  [DIAGNOSTIC] 'package_count' column found in DB schema for {data_type}.")

    # 3. Prepare package counts for calculation
    # Ensure numeric (fetch_data should have done this, but defensive check)
    package_counts = pd.to_numeric(df['package_count'], errors='coerce')
    
    # 4. Enforce the rule: If missing (NaN) or invalid (<=0), default to 1 package.
    # Fill NaNs (missing data) with 1
    package_counts = package_counts.fillna(1)
    
    # Enforce minimum of 1 (handles 0 or negative values robustly using clip)
    package_counts = package_counts.clip(lower=1)

    # 5. Perform the calculation
    logging.info(f"  [DIAGNOSTIC] Applying calculation: Total Weight = quantity_kgs * package_count.")
    df['total_weight_kgs'] = df['quantity_kgs'] * package_counts
    
    return df


def prepare_sales_data(sales_df_raw):
    if sales_df_raw.empty: return pd.DataFrame()

    # Apply the Fix for Total Weight Calculation
    sales_df = calculate_total_weight(sales_df_raw, data_type="SALES")
    
    essential_cols = ['total_weight_kgs', 'price', 'sale_number', 'lot_number']
    
    if not all(col in sales_df.columns for col in essential_cols):
        logging.warning("Missing essential columns after weight calculation (Total KGs, Price, Sale#, Lot#)."); return pd.DataFrame()

    # Filter out rows where essential data (including the result of the calculation) is missing or zero
    # Note: 'price' should be numeric from fetch_data, but we check again defensively.
    sales_df['price'] = pd.to_numeric(sales_df['price'], errors='coerce')

    sales_df = sales_df.dropna(subset=essential_cols)
    sales_df = sales_df[(sales_df['total_weight_kgs'] > 0) & (sales_df['price'] > 0)]

    if sales_df.empty: return pd.DataFrame()
    
    # Calculate total value
    # Use .copy() to avoid SettingWithCopyWarning
    sales_df = sales_df.copy()
    sales_df['value_usd'] = sales_df['price'] * sales_df['total_weight_kgs']

    analytical_cols = ['mark', 'grade', 'buyer', 'broker']
    for col in analytical_cols:
        if col in sales_df.columns:
            sales_df[col] = sales_df[col].astype(str).fillna(PLACEHOLDER)
            sales_df[col] = sales_df[col].replace(['nan', '<NA>'], PLACEHOLDER)
        # Ensure columns exist even if missing in raw data
        else:
            sales_df[col] = PLACEHOLDER
            
    return sales_df

def prepare_offers_data(offers_df_raw):
    if offers_df_raw.empty: return pd.DataFrame()

    # Apply the Fix for Total Weight Calculation
    offers_df = calculate_total_weight(offers_df_raw, data_type="OFFERS")

    # Filter out rows with zero weight
    if 'total_weight_kgs' in offers_df.columns:
        offers_df = offers_df[offers_df['total_weight_kgs'] > 0]
    
    return offers_df


# =============================================================================
# Analysis Functions (KPIs and Forecast)
# =============================================================================

# ... (The rest of the script remains identical to the user's provided version) ...

def analyze_kpis_and_forecast(sales_df_week, sales_df_all, sales_df_week_raw, offers_df_week):
    kpis = {}; tables = {'sell_through': [], 'realization': []}

    # KPIs rely on the prepared 'sales_df_week' which now has the corrected 'total_weight_kgs'
    if sales_df_week.empty or 'total_weight_kgs' not in sales_df_week.columns:
        kpis['TOTAL_VOLUME'] = "0"; kpis['AVG_PRICE'] = "$0.00"
        kpis['PRICE_CHANGE'] = "N/A"; kpis['PRICE_CHANGE_CLASS'] = 'neutral'; kpis['PRICE_CHANGE_NUMERIC'] = 0
    else:
        total_volume = sales_df_week['total_weight_kgs'].sum()
        avg_price = sales_df_week['value_usd'].sum() / total_volume if total_volume > 0 else 0
        kpis['TOTAL_VOLUME'] = f"{total_volume:,.0f}"; kpis['AVG_PRICE'] = f"${avg_price:.2f}"

        if not sales_df_week.empty:
            current_sale_number = sales_df_week['sale_number'].iloc[0]
            prev_week_df = get_previous_week_df(sales_df_all, current_sale_number)
            if not prev_week_df.empty and 'value_usd' in prev_week_df.columns and 'total_weight_kgs' in prev_week_df.columns:
                prev_volume = prev_week_df['total_weight_kgs'].sum()
                prev_avg_price = prev_week_df['value_usd'].sum() / prev_volume if prev_volume > 0 else 0
                if prev_avg_price > 0:
                    change = ((avg_price - prev_avg_price) / prev_avg_price) * 100
                    kpis['PRICE_CHANGE_NUMERIC'] = change
                    kpis['PRICE_CHANGE'] = f"{change:+.2f}%"
                    if change > 0.5: kpis['PRICE_CHANGE_CLASS'] = 'positive'
                    elif change < -0.5: kpis['PRICE_CHANGE_CLASS'] = 'negative'
                    else: kpis['PRICE_CHANGE_CLASS'] = 'neutral'

        if 'PRICE_CHANGE' not in kpis:
            kpis['PRICE_CHANGE'] = "N/A (First Sale)"; kpis['PRICE_CHANGE_CLASS'] = 'neutral'; kpis['PRICE_CHANGE_NUMERIC'] = 0

    # Calculate Sell-Through based on unique lots 
    
    # Determine the current sale number from available data
    current_sale_number = None
    if not sales_df_week.empty:
        current_sale_number = sales_df_week['sale_number'].iloc[0]
    elif not offers_df_week.empty:
        current_sale_number = offers_df_week['sale_number'].iloc[0]

    lots_offered = 0; lots_sold = 0
    
    # Calculate Lots Offered (using prepared offers_df_week)
    if not offers_df_week.empty and all(col in offers_df_week.columns for col in ['broker', 'lot_number', 'sale_number']):
        # Filtering by sale number is already done when creating offers_df_week in main()
        lots_offered = offers_df_week[['broker', 'lot_number']].drop_duplicates().shape[0]

    # Calculate Lots Sold (using raw sales_df_week_raw)
    if not sales_df_week_raw.empty and all(col in sales_df_week_raw.columns for col in ['broker', 'lot_number', 'sale_number']):
         # We must filter the raw data by the current sale number if determined
        if current_sale_number:
            # Ensure 'sale_number' is treated as string in raw data for comparison
            # We need to copy the raw df before modifying it to avoid SettingWithCopyWarning
            sales_df_week_raw_copy = sales_df_week_raw.copy()
            
            # Check if the column exists before trying to cast type (safety check)
            if 'sale_number' in sales_df_week_raw_copy.columns:
                sales_df_week_raw_copy['sale_number'] = sales_df_week_raw_copy['sale_number'].astype(str)
                
                # Filter raw data safely
                sales_calc = sales_df_week_raw_copy[sales_df_week_raw_copy['sale_number'] == current_sale_number]
                
                # We count unique lots sold, regardless of whether they passed the price/kg filters for analysis.
                lots_sold = sales_calc[['broker', 'lot_number']].drop_duplicates().shape[0]

    sell_through_rate = (lots_sold / lots_offered) if lots_offered > 0 else 0
    kpis['SELL_THROUGH_RATE'] = f"{sell_through_rate:.2%}"; kpis['SELL_THROUGH_RATE_RAW'] = sell_through_rate
    tables['sell_through'].append({'Metric': 'Lots Offered', 'Value': f"{lots_offered:,.0f}"})
    tables['sell_through'].append({'Metric': 'Lots Sold', 'Value': f"{lots_sold:,.0f}"})
    tables['sell_through'].append({'Metric': 'Rate', 'Value': kpis['SELL_THROUGH_RATE']})

    kpis['REALIZATION_RATE'] = 'N/A'
    tables['realization'].append({'Metric': 'Status', 'Value': 'Insufficient Data'})
    kpis['SNAPSHOT'] = generate_snapshot(kpis)
    return kpis, tables

def generate_snapshot(kpis):
    if kpis.get('TOTAL_VOLUME') == '0': return "Awaiting sales results. Offers published."
    price_change = kpis.get('PRICE_CHANGE_NUMERIC', 0)
    if price_change > 1.5: price_desc = "Prices significantly higher"
    elif price_change > 0.5: price_desc = "Prices firm to dearer"
    elif price_change < -1.5: price_desc = "Prices significantly lower"
    elif price_change < -0.5: price_desc = "Prices easier"
    else: price_desc = "Prices generally steady"

    sell_through_rate = kpis.get('SELL_THROUGH_RATE_RAW', 0)
    if sell_through_rate > 0.95: demand_desc = "Excellent absorption."
    elif sell_through_rate >= 0.85: demand_desc = "Good general demand."
    elif sell_through_rate >= 0.75: demand_desc = "Fair demand; selective buying."
    else: demand_desc = "Low demand; significant withdrawals."

    return f"{price_desc} ({kpis.get('PRICE_CHANGE', 'N/A')}). {demand_desc}"

# =============================================================================
# Interactive Chart Generation (NAMED DATA SOURCES)
# =============================================================================

def create_price_distribution_chart(brush):
    """Creates the main price distribution histogram using named data."""
    height = 220

    # Use the named data source
    chart = alt.Chart(alt.NamedData(DATA_SOURCE_WEEK)).mark_bar(color=PRIMARY_COLOR).encode(
        x=alt.X('price:Q', bin=alt.Bin(maxbins=50), title='Price (USD/kg)'),
        y=alt.Y('count():Q', title='Number of Lots'),
        opacity=alt.condition(brush, alt.value(1.0), alt.value(0.7)),
        tooltip=[alt.Tooltip('price:Q', bin=True), alt.Tooltip('count():Q')]
    ).properties(
        title="Price Distribution (Click and drag here to filter other charts)",
        height=height,
        width='container'
    ).add_params(
        brush
    )
    return chart

def create_grade_performance_chart(brush):
    """Creates the grade performance chart using named data sources."""
    
    base_chart = alt.Chart(alt.NamedData(DATA_SOURCE_WEEK))
    
    tooltip_fields = [
            alt.Tooltip('grade:N'), 
            alt.Tooltip('mean(price):Q', format='$.2f', title='Current Avg Price (Filtered)'), 
            alt.Tooltip('sum(total_weight_kgs):Q', format=',.0f', title='Current Volume (Filtered)')
        ]

    base_chart = base_chart.transform_lookup(
        lookup='grade',
        from_=alt.LookupData(data=alt.NamedData(DATA_SOURCE_PREV_GRADE), key='grade', fields=['prev_avg_price']),
        default=None
    )
    tooltip_fields.append(alt.Tooltip('max(prev_avg_price):Q', format='$.2f', title='Previous Avg Price (Overall)'))


    # 1. Current Week Bars (Filtered by the brush)
    bars = base_chart.mark_bar(color=PRIMARY_COLOR).encode(
        x=alt.X('grade:N', title='Grade', sort=alt.EncodingSortField(field="price", op="mean", order='descending')),
        y=alt.Y('mean(price):Q', title='Average Price (USD/kg)'),
        tooltip=tooltip_fields
    ).transform_filter(
        brush
    )

    # 2. Previous Week Ticks (Static, NOT filtered by the brush)
    title_suffix = " (Black tick = Previous Week)"
    ticks = alt.Chart(alt.NamedData(DATA_SOURCE_PREV_GRADE)).mark_tick(
        color=HISTORICAL_TICK_COLOR, thickness=2, size=15
    ).encode(
        x='grade:N',
        y='prev_avg_price:Q'
    )
    
    # Layer the ticks over the bars
    chart = alt.layer(bars, ticks)
    
    return chart.properties(
        title=f"Average Price by Grade{title_suffix}",
        height=CHART_HEIGHT,
        width='container'
    ).add_params(
        brush
    )

def create_broker_performance_chart(brush):
    """Creates the broker performance chart using named data sources."""

    base_chart = alt.Chart(alt.NamedData(DATA_SOURCE_WEEK))

    tooltip_fields = [
            alt.Tooltip('broker:N'), 
            alt.Tooltip('sum(value_usd):Q', format='$,.0f', title='Current Total Value (Filtered)'), 
            alt.Tooltip('mean(price):Q', format='$.2f', title='Current Avg Price (Filtered)')
        ]

    base_chart = base_chart.transform_lookup(
        lookup='broker',
        from_=alt.LookupData(data=alt.NamedData(DATA_SOURCE_PREV_BROKER), key='broker', fields=['prev_total_value']),
        default=None
    )
    tooltip_fields.append(alt.Tooltip('max(prev_total_value):Q', format='$,.0f', title='Previous Total Value (Overall)'))


    # 1. Current Week Bars (Filtered by the brush)
    bars = base_chart.mark_bar(color=PRIMARY_COLOR).encode(
        x=alt.X('broker:N', title='Broker', sort=alt.EncodingSortField(field="value_usd", op="sum", order='descending')),
        y=alt.Y('sum(value_usd):Q', title='Total Value (USD)'),
        tooltip=tooltip_fields
    ).transform_filter(
        brush
    )

    # 2. Previous Week Ticks (Static, NOT filtered by the brush)
    title_suffix = " (Black tick = Previous Week)"
    ticks = alt.Chart(alt.NamedData(DATA_SOURCE_PREV_BROKER)).mark_tick(
        color=HISTORICAL_TICK_COLOR, thickness=2, size=15
    ).encode(
        x='broker:N',
        y='prev_total_value:Q'
    )
    # Layer the ticks over the bars
    chart = alt.layer(bars, ticks)

    return chart.properties(
        title=f"Total Value by Broker{title_suffix}",
        height=CHART_HEIGHT,
        width='container'
    ).add_params(
        brush
    )

def create_interactive_analysis_components():
    """
    Coordinates the interactive charts structure (data is injected later).
    """
    # Define the shared brush (selection).
    brush = alt.param(
        name='price_brush',
        select={
            'type': 'interval',
            'encodings': ['x'],
            'fields': ['price']
        }
    )

    # Create the individual components, passing the shared brush
    distribution_chart = create_price_distribution_chart(brush)
    grade_chart = create_grade_performance_chart(brush)
    broker_chart = create_broker_performance_chart(brush)

    # Return the components as a dictionary of JSON specifications
    return {
        'distribution': distribution_chart.to_dict(),
        'grade': grade_chart.to_dict(),
        'broker': broker_chart.to_dict()
    }

def create_buyer_components():
    """Generates interactive buyer charts using named data sources and dynamic ranking."""
    
    # 1. Define Interactions (Shared)
    LAYOUT_HEIGHT = 400

    # 2a. Buyer Selection (Drill-down)
    buyer_selection = alt.param(
        name='buyer_select',
        select={
            'type': 'point',
            'fields': ['buyer'],
            'clear': False
        }
    )

    # 2b. Value/Volume Switch (Radio buttons)
    metric_options = ['Value (USD)', 'Volume (kg)']
    metric_binding = alt.binding_radio(options=metric_options, name='Select Metric: ')
    metric_switch = alt.param(bind=metric_binding, value=metric_options[0], name='metric_switch_param')

    # 3. Main Buyer Chart (Overview)
    main_chart_base = alt.Chart(alt.NamedData(DATA_SOURCE_WEEK))

    # Filter out placeholder data
    main_chart_base = main_chart_base.transform_filter(
        alt.datum.buyer != PLACEHOLDER
    )

    # Calculate the dynamic metric first
    main_chart_base = main_chart_base.transform_calculate(
         dynamic_metric=alt.expr.if_(metric_switch == metric_options[0], alt.datum.value_usd, alt.datum.total_weight_kgs)
    )

    # Apply Top N transformation (Aggregate -> Window Rank -> Filter)
    main_chart_base = main_chart_base.transform_aggregate(
        total_value='sum(value_usd)',
        total_volume='sum(total_weight_kgs)',
        dynamic_metric_sum='sum(dynamic_metric)',
        groupby=['buyer']
    ).transform_window(
        # Rank based on the currently selected metric
        rank='rank()',
        sort=[alt.SortField('dynamic_metric_sum', order='descending')]
    ).transform_filter(
        # Keep only the Top 15
        alt.datum.rank <= 15
    )

    # Calculate Avg Price after aggregation
    main_chart_base = main_chart_base.transform_calculate(
        avg_price=alt.datum.total_value / alt.datum.total_volume
    )


    main_chart = main_chart_base.mark_bar().encode(
        # Sort Y axis based on the currently selected metric
        y=alt.Y('buyer:N', title='Buyer', sort=alt.SortField(field="dynamic_metric_sum", order="descending")),
        x=alt.X('dynamic_metric_sum:Q').title(None),
        color=alt.condition(buyer_selection, alt.value(PRIMARY_COLOR), alt.value(LIGHTER_BLUE)),
        tooltip=[
            alt.Tooltip('buyer:N'),
            alt.Tooltip('total_value:Q', format='$,.0f', title='Value (USD)'),
            alt.Tooltip('total_volume:Q', format=',.0f', title='Volume (kg)'),
            alt.Tooltip('avg_price:Q', format='$.2f', title='Avg Price')
        ]
    ).properties(
        title="Top 15 Buyers (Click bar to see breakdown)",
        height=LAYOUT_HEIGHT,
        width='container'
    ).add_params(
        buyer_selection,
        metric_switch
    )

    # 4. Drill-down Chart (Grade Breakdown)
    grade_breakdown = alt.Chart(alt.NamedData(DATA_SOURCE_WEEK)).mark_bar(color=PRIMARY_COLOR).encode(
        y=alt.Y('grade:N', title='Grade', sort='-x'),
        x=alt.X('sum(dynamic_metric):Q').title(None),
        tooltip=[
            alt.Tooltip('buyer:N'),
            alt.Tooltip('grade:N'),
            alt.Tooltip('sum(value_usd):Q', format='$,.0f', title='Value (USD)'),
            alt.Tooltip('sum(total_weight_kgs):Q', format=',.0f', title='Volume (kg)')
        ]
    ).transform_calculate(
         dynamic_metric=alt.expr.if_(metric_switch == metric_options[0], alt.datum.value_usd, alt.datum.total_weight_kgs)
    ).transform_filter(
        buyer_selection
    ).properties(
        title="Grade Breakdown",
        height=LAYOUT_HEIGHT,
        width='container'
    ).add_params(
        buyer_selection,
        metric_switch
    )
    
    # 5. Return independent components
    return {
        'main': main_chart.to_dict(),
        'breakdown': grade_breakdown.to_dict()
    }


# =============================================================================
# Advanced Analysis (Candlestick and Insights)
# =============================================================================

def analyze_price_movements(sales_df_week, sales_df_all):
    required_cols = ['price', 'total_weight_kgs', 'sale_number']
    if sales_df_week.empty or not all(c in sales_df_week.columns for c in required_cols):
        return pd.DataFrame(), "Awaiting current week data or missing key columns for trend analysis."

    if sales_df_week.empty:
         return pd.DataFrame(), "Awaiting current week data for trend analysis."
         
    current_sale_number = sales_df_week['sale_number'].iloc[0]
    prev_week_df = get_previous_week_df(sales_df_all, current_sale_number)

    if prev_week_df.empty or not all(c in prev_week_df.columns for c in required_cols):
        return pd.DataFrame(), "First sale recorded; no historical data for comparison."

    # Grouping by 'mark' and 'grade' which are guaranteed to exist now
    current_metrics = sales_df_week.groupby(['mark', 'grade']).agg(
        close=('price', 'mean'), high=('price', 'max'), low=('price', 'min'), volume=('total_weight_kgs', 'sum')
    ).reset_index()

    prev_metrics = prev_week_df.groupby(['mark', 'grade']).agg(open=('price', 'mean')).reset_index()
    movement_df = pd.merge(current_metrics, prev_metrics, on=['mark', 'grade'], how='inner')
    
    if movement_df.empty:
        return pd.DataFrame(), "No common items sold between this week and the previous week for comparison."

    movement_df['change'] = movement_df['close'] - movement_df['open']
    movement_df['change_pct'] = (movement_df['change'] / movement_df['open']) * 100
    movement_df['color'] = movement_df.apply(lambda row: '#34a853' if row['change'] >= 0 else '#ea4335', axis=1)

    insights = []
    significant_volume_df = movement_df[(movement_df['volume'] > 500) & (movement_df['mark'] != PLACEHOLDER)]
    top_risers = significant_volume_df.sort_values(by='change_pct', ascending=False).head(3)
    top_fallers = significant_volume_df.sort_values(by='change_pct', ascending=True).head(3)

    if not top_risers.empty:
        insights.append("Notable price increases week-over-week (min 500kg):")
        for _, row in top_risers.iterrows():
            insights.append(f"- {row['mark']} ({row['grade']}) rose by {row['change_pct']:.1f}% (from ${row['open']:.2f} to ${row['close']:.2f}).")

    if not top_fallers.empty:
        if insights: insights.append("\n")
        insights.append("Significant price declines week-over-week (min 500kg):")
        for _, row in top_fallers.iterrows():
             insights.append(f"- {row['mark']} ({row['grade']}) decreased by {row['change_pct']:.1f}% (from ${row['open']:.2f} to ${row['close']:.2f}).")

    if not insights:
        insights.append("Prices across most compared gardens and grades remained relatively stable week-over-week.")

    return movement_df, "\n".join(insights)


def create_candlestick_chart(marks):
    """Generates the Candlestick chart specification using named data."""
    if not marks:
        return {}

    input_dropdown = alt.binding_select(options=marks, name='Select Garden: ')
    
    # Define the selection parameter
    selection = alt.param(
        name='garden_select',
        select={
            'type': 'point',
            'fields': ['mark']
        },
        bind=input_dropdown,
        value=[{'mark': marks[0]}] # Initialize with the first mark
    )

    # Base chart definition
    base = alt.Chart(alt.NamedData(DATA_SOURCE_MOVEMENT)).transform_filter(
        selection
    ).properties(
        width='container',
        height=400,
        title="Week-over-Week Price Movement (Candlestick)"
    )

    # Define Tooltip fields (reusable)
    tooltip_fields = [
            alt.Tooltip('mark:N'), alt.Tooltip('grade:N'),
            alt.Tooltip('open:Q', format='$.2f', title='Previous Avg (Open)'), 
            alt.Tooltip('close:Q', format='$.2f', title='Current Avg (Close)'),
            alt.Tooltip('high:Q', format='$.2f'), alt.Tooltip('low:Q', format='$.2f'),
            alt.Tooltip('change_pct:Q', format='+.2f', title='Change %')
        ]

    # Define X-axis encoding
    x_encoding = alt.X('grade:N', axis=alt.Axis(labelAngle=-45), scale=alt.Scale(paddingInner=0.3))

    # 1. The Wicks
    wicks = base.mark_rule(strokeWidth=1).encode(
        x=x_encoding,
        y=alt.Y('low:Q', title='Price (USD/kg)', scale=alt.Scale(zero=False)),
        y2='high:Q',
        color=alt.Color('color:N', scale=None),
        tooltip=tooltip_fields
    )

    # 2. The Body
    body = base.mark_bar().encode(
        x=x_encoding,
        y='open:Q',
        y2='close:Q',
        color=alt.Color('color:N', scale=None),
        tooltip=tooltip_fields
    )

    # Combine layers and add the selection mechanism
    chart = alt.layer(wicks, body).add_params(selection)

    return chart.to_dict()


# =============================================================================
# Data Export and Forward Outlook
# =============================================================================

def generate_raw_data_export(sales_df_week):
    if sales_df_week.empty: return []
    cols_to_export = ['mark', 'grade', 'lot_number', 'total_weight_kgs', 'price', 'buyer', 'broker']
    available_cols = [col for col in cols_to_export if col in sales_df_week.columns]
    if not available_cols: return []
    export_df = sales_df_week[available_cols].copy()
    rename_map = {
        'mark': 'Mark', 'grade': 'Grade', 'lot_number': 'Lot', 'total_weight_kgs': 'KGs',
        'price': 'Price (USD)', 'buyer': 'Buyer', 'broker': 'Broker'
    }
    export_df = export_df.rename(columns=rename_map)
    if 'Lot' in export_df.columns:
        # Handle cases where Lot might be numeric or mixed types before converting to string
        export_df['Lot'] = export_df['Lot'].fillna('').astype(str)

    export_df = export_df.replace({np.nan: None})
    return export_df.to_dict(orient='records')

def generate_forecast_outlook(week_number, location, offers_df_all):
    outlook = {
        "next_sale": "N/A", "forthcoming_offerings_kgs": "Awaiting Catalogues",
        "weather_outlook": f"Seasonal weather patterns are prevailing in the key growing regions supplying {location}. Production levels are reported as stable.",
        "market_prediction": "Based on current demand trends, the market is expected to remain active. Buyers are advised to monitor global economic indicators and currency fluctuations which may impact pricing in the coming weeks."
    }
    if not week_number or offers_df_all.empty or 'sale_number' not in offers_df_all.columns: return outlook

    try:
        current_week_str = str(week_number)
        # Ensure comparison is string-based for 'YYYY-WW' format
        future_sales = sorted([s for s in offers_df_all['sale_number'].dropna().unique() if str(s) > current_week_str])
    except TypeError:
        logging.warning("Could not compare sale numbers (type mismatch). Skipping forecast calculation."); return outlook

    if future_sales:
        next_sale_number = future_sales[0]
        outlook["next_sale"] = str(next_sale_number)
        next_week_offers = offers_df_all[offers_df_all['sale_number'] == next_sale_number]
        if 'total_weight_kgs' in next_week_offers.columns:
            forthcoming_volume = next_week_offers['total_weight_kgs'].sum()
            if pd.notna(forthcoming_volume) and forthcoming_volume > 0:
                outlook["forthcoming_offerings_kgs"] = f"{forthcoming_volume:,.0f}"
    return outlook


# =============================================================================
# Main Processing Loop
# =============================================================================

def main():
    logging.info("Starting Mombasa Data Analysis (V12 Comprehensive Diagnostics)...")

    # V12: Ensure the output directory exists
    if not os.path.exists(DATA_OUTPUT_DIR):
        try:
            os.makedirs(DATA_OUTPUT_DIR)
            logging.info(f"[DIAGNOSTIC] Created output directory: {DATA_OUTPUT_DIR}")
        except OSError as e:
            logging.error(f"Could not create output directory {DATA_OUTPUT_DIR}: {e}")
            sys.exit(1)

    conn = connect_db()
    # Fetch raw data from the database
    sales_df_raw, offers_df_raw = fetch_data(conn)
    
    # Prepare (Clean and Calculate) the data
    # This step includes the calculation fixes and diagnostics.
    sales_df_all = prepare_sales_data(sales_df_raw)
    offers_df_all = prepare_offers_data(offers_df_raw)

    # Determine unique weeks present in the data
    all_weeks = []
    if 'sale_number' in sales_df_raw.columns and not sales_df_raw.empty:
        all_weeks.extend(sales_df_raw['sale_number'].dropna().unique())
    if 'sale_number' in offers_df_raw.columns and not offers_df_raw.empty:
        all_weeks.extend(offers_df_raw['sale_number'].dropna().unique())

    # Ensure consistent sorting (string sort is safest for 'YYYY-WW' format)
    all_weeks = sorted([str(w) for w in list(set(all_weeks))])

    if len(all_weeks) == 0:
        logging.info("No sale data found in database. Exiting."); return

    report_index = []

    # Process each week individually
    for week_number in all_weeks:
        # Ensure week_number is treated as a string for consistent filtering
        week_number_str = str(week_number)
        logging.info(f"Processing Sale: {week_number_str}")

        # Filter data for the specific week
        # Note: We pass the RAW dataframes here for specific needs (like sell-through calculation)
        sales_week_raw = sales_df_raw[sales_df_raw['sale_number'] == week_number_str] if not sales_df_raw.empty else pd.DataFrame()
        
        # We use the PREPARED dataframes for analysis
        offers_week = offers_df_all[offers_df_all['sale_number'] == week_number_str] if not offers_df_all.empty else pd.DataFrame()
        sales_week = sales_df_all[sales_df_all['sale_number'] == week_number_str] if not sales_df_all.empty else pd.DataFrame()

        # Metadata
        location = 'Mombasa'; week_date = "Unknown"; year = "Unknown"
        # Try getting date from sales data first (using raw as it's more likely to have the date even if filtered out later)
        if not sales_week_raw.empty and 'sale_date' in sales_week_raw.columns and not sales_week_raw['sale_date'].dropna().empty:
             week_date = sales_week_raw['sale_date'].dropna().iloc[0]
        # Fallback to offers data
        elif not offers_week.empty and 'sale_date' in offers_week.columns and not offers_week['sale_date'].dropna().empty:
            week_date = offers_week['sale_date'].dropna().iloc[0]

        if week_date != "Unknown" and week_date is not pd.NA and week_date:
            try: year = pd.to_datetime(week_date).year
            except Exception as e: logging.warning(f"Could not parse date '{week_date}': {e}")

        try: 
            # Attempt to extract the numerical week part (e.g., 39 from 2025-39)
            sale_num_only = int(week_number_str.split('-')[1])
        except (IndexError, ValueError): 
            sale_num_only = week_number_str

        # Run Analysis (KPIs and Forecast)
        kpis, forecast_tables = analyze_kpis_and_forecast(sales_week, sales_df_all, sales_week_raw, offers_week)

        # Advanced Analysis
        movement_data, analytical_insights = analyze_price_movements(sales_week, sales_df_all)

        # Calculate Historical Metrics (Needed for data sources)
        prev_week_df = get_previous_week_df(sales_df_all, week_number_str)
        prev_week_grade_metrics = pd.DataFrame()
        prev_week_broker_metrics = pd.DataFrame()

        if not prev_week_df.empty:
            # Columns 'grade', 'price', 'broker', 'value_usd' are ensured by prepare_sales_data
            prev_week_grade_metrics = prev_week_df.groupby('grade').agg(
                prev_avg_price=('price', 'mean')
            ).reset_index()
            
            prev_week_broker_metrics = prev_week_df.groupby('broker').agg(
                prev_total_value=('value_usd', 'sum')
            ).reset_index()

        # Calculate unique marks for the candlestick dropdown
        unique_marks = []
        if not movement_data.empty and 'mark' in movement_data.columns:
             unique_marks = sorted([m for m in movement_data['mark'].unique().tolist() if m != PLACEHOLDER])


        # Generate Charts (Structural definition only)
        charts = {}
        
        # Interactive Analysis Components
        interactive_components = create_interactive_analysis_components()
        charts['interactive_distribution'] = interactive_components['distribution']
        charts['interactive_grade'] = interactive_components['grade']
        charts['interactive_broker'] = interactive_components['broker']
        
        # Buyer Components
        buyer_components = create_buyer_components()
        charts['buyers_main'] = buyer_components['main']
        charts['buyers_breakdown'] = buyer_components['breakdown']

        # Candlestick (Pass the unique marks for the dropdown)
        charts['candlestick'] = create_candlestick_chart(unique_marks)

        tables = {
            'sell_through': forecast_tables['sell_through'],
            'realization': forecast_tables['realization'],
            'raw_sales_data': generate_raw_data_export(sales_week)
        }
        
        outlook = generate_forecast_outlook(week_number_str, location, offers_df_all)

        # Prepare data sources for embedding
        # Convert dataframes to records (list of dictionaries) for efficient JSON storage
        # We must handle potential NaN values during conversion for JSON compatibility.
        data_sources = {
            DATA_SOURCE_WEEK: sales_week.replace({np.nan: None}).to_dict(orient='records'),
            DATA_SOURCE_PREV_GRADE: prev_week_grade_metrics.replace({np.nan: None}).to_dict(orient='records'),
            DATA_SOURCE_PREV_BROKER: prev_week_broker_metrics.replace({np.nan: None}).to_dict(orient='records'),
            DATA_SOURCE_MOVEMENT: movement_data.replace({np.nan: None}).to_dict(orient='records')
        }


        # Structure the report data
        report_data = {
            'metadata': {
                'sale_number': week_number_str,
                'sale_date': week_date, 'location': location,
                'year': year, 'sale_num_only': sale_num_only, 'generated_at': datetime.datetime.now().isoformat()
            },
            'kpis': kpis,
            'insights': analytical_insights,
            'charts': charts,
            'tables': tables,
            'outlook': outlook,
            'data_sources': data_sources # Add the centralized data sources
        }

        # Save the report JSON file
        filename = f"mombasa_{week_number_str.replace('-', '_')}.json"
        filepath = os.path.join(DATA_OUTPUT_DIR, filename)

        try:
            with open(filepath, 'w') as f:
                # Use default=str for any remaining complex types (like datetime if any slipped through)
                json.dump(report_data, f, indent=2, default=str)

            # Add details to index
            report_index.append({
                'sale_number': week_number_str,
                'sale_num_only': sale_num_only,
                'sale_date': week_date,
                'year': year,
                'filename': filename,
                'location': location,
                'snapshot': kpis.get('SNAPSHOT', 'Awaiting Data.')
            })
        except Exception as e:
            logging.error(f"Error saving JSON for {week_number_str}: {e}", exc_info=True)

    # Save the index file
    try:
        # Sort by sale_number string descending (Newest first)
        report_index.sort(key=lambda x: x['sale_number'], reverse=True)
        with open(INDEX_FILE, 'w') as f:
            json.dump(report_index, f, indent=2)
        logging.info(f"Generated index file: {INDEX_FILE} with {len(report_index)} entries.")
    except Exception as e:
        logging.error(f"Error saving index file: {e}")

    if conn:
        conn.close()
    logging.info("Analysis Complete.")

if __name__ == "__main__":
    main()