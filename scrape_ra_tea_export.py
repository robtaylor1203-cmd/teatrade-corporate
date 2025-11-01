import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import time
import os
import logging

# =============================================================================
# Configuration
# =============================================================================

TARGET_URL = "https://www.rainforest-alliance.org/business/certification/tea-certification-data-report-2024/"

# This is the internal title (aria-label) of the main chart on the first page.
# To target other visuals, you would need to find their specific aria-label.
TARGET_VISUAL_TITLE = "Certified tea volumes. Visual."
TIMEOUT = 45000 # 45 seconds for slow operations (like iframe loading or download)

# Define the output directory
OUTPUT_DIR = os.path.abspath("ra_data_export")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(level=logging.INFO, format='PBI_SCRAPER: %(levelname)s: %(message)s')

# =============================================================================
# Scraping Logic
# =============================================================================

def scrape_powerbi_export(page):
    """
    Attempts to export data from a specific Power BI visual within the iframe.
    """
    logging.info(f"Attempting to export data for: '{TARGET_VISUAL_TITLE}'")

    # 1. Locate and Wait for the Power BI Iframe
    try:
        # We look for an iframe whose source URL contains 'app.powerbi.com'
        iframe_locator = page.frame_locator("iframe[src*='app.powerbi.com']")
        # Wait for the core content container inside the iframe to load
        iframe_locator.locator(".report-container").wait_for(timeout=TIMEOUT)
        logging.info("Iframe located and report container loaded.")
    except Exception as e:
        logging.error(f"Error locating or loading the Power BI iframe: {e}")
        return False

    # 2. Locate and Hover over the Target Visualization
    try:
        # Find the visual container by its accessibility label (aria-label)
        visual_container = iframe_locator.locator(f"[aria-label='{TARGET_VISUAL_TITLE}']").first
        
        # We must hover over the visual for the options menu button to appear
        visual_container.hover(timeout=10000)
        logging.info(f"Hovering over '{TARGET_VISUAL_TITLE}'.")
    except Exception as e:
        logging.error(f"Error locating or hovering over the visual '{TARGET_VISUAL_TITLE}': {e}")
        return False

    # 3. Click the "More Options" button
    try:
        # The 'More options' button (three dots) appears after the hover
        # We use a selector based on its title attribute
        more_options_button = iframe_locator.get_by_title("More options")
        more_options_button.click(timeout=10000)
        logging.info("Clicked 'More options'.")
    except Exception as e:
        logging.error(f"Error clicking 'More options' button. It might not have appeared after hover: {e}")
        return False

    # 4. Click "Export data" in the menu
    try:
        # Locate the menu item for exporting data (based on its title)
        export_data_menu_item = iframe_locator.get_by_title("Export data")
        export_data_menu_item.click(timeout=10000)
        logging.info("Clicked 'Export data'. Waiting for dialog.")
    except Exception as e:
        logging.error(f"Error clicking 'Export data' menu item. It might be disabled or the menu structure changed: {e}")
        return False

    # 5. Handle the Export Dialog and Download
    try:
        # We must set up an expectation for the download event *before* we click the final button
        with page.expect_download(timeout=TIMEOUT) as download_info:
            # Click the final "Export" button in the dialog box (using role and name)
            export_button = iframe_locator.get_by_role("button", name="Export")
            export_button.click(timeout=10000)
            logging.info("Clicked final 'Export' button. Waiting for download to initiate...")

        download = download_info.value
        
        # Save the file
        original_filename = download.suggested_filename
        save_path = os.path.join(OUTPUT_DIR, original_filename)
        download.save_as(save_path)
        logging.info(f"--- SUCCESS ---")
        logging.info(f"Successfully downloaded data to: {save_path}")
        return True

    except PlaywrightTimeoutError:
        logging.error("Timeout waiting for the download to start after clicking Export.")
        return False
    except Exception as e:
        logging.error(f"Error during the download process or clicking the final Export button: {e}")
        return False

# =============================================================================
# Main Execution
# =============================================================================

def run():
    """Main execution function."""
    
    # Use headless=False so you can watch the browser actions and debug if necessary
    with sync_playwright() as p:
        # Chromium is generally best for Power BI compatibility
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        try:
            logging.info(f"Navigating to {TARGET_URL}...")
            # Increase timeout as the page is slow
            page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)

            # Handle potential cookie banners
            try:
                # Look for common acceptance button texts
                accept_cookies = page.get_by_role("button", name="Accept All Cookies", exact=True).or_(
                                 page.get_by_role("button", name="Accept", exact=True))
                
                if accept_cookies.is_visible(timeout=5000):
                    accept_cookies.click()
                    logging.info("Cookie banner handled.")
            except:
                pass # Ignore if banner not found

            # Scroll down slightly to ensure the iframe initiates loading
            page.evaluate("window.scrollBy(0, 350);")

            # Execute the scraping logic
            scrape_powerbi_export(page)

        except Exception as e:
            logging.error(f"An unexpected error occurred during navigation or setup: {e}")
        
        finally:
            # Keep the browser open for a moment for inspection
            logging.info("Pausing briefly before closing browser.")
            time.sleep(5)
            browser.close()

if __name__ == "__main__":
    run()