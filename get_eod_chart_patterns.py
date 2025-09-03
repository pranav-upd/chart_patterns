import logging
import os
import time
import re
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Assuming these modules exist based on the provided context
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.signals.sg_eod_chart_patterns import SgEodChartPatternsRepository
from algo_scripts.algotrade.scripts.trade_utils.time_manager import get_current_ist_time_as_str, get_screener_run_id, get_ist_time
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import get_db_session

# ---------------- Load Env ---------------- #
load_dotenv()

# ---------------- Setup Logging ---------------- #
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("GET_EOD_CHART_PATTERNS")

INTRADAY_SCREENER_EMAIL = os.getenv("INTRADAY_SCREENER_EMAIL")
INTRADAY_SCREENER_PWD = os.getenv("INTRADAY_SCREENER_PWD")

def run_scraper():
    """
    Handles all Selenium browser interactions: setup, login, navigation, and scraping.
    """
    logger.info("🚀 Launching browser...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    scraped_data = []

    try:
        driver.get("https://intradayscreener.com/login")
        logger.info("🌐 Opened login page.")

        email_field = WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.XPATH, '//input[@type="email"]')))
        email_field.send_keys(INTRADAY_SCREENER_EMAIL)
        password_field = driver.find_element(By.XPATH, '//input[@type="password"]')
        password_field.send_keys(INTRADAY_SCREENER_PWD)
        login_button = driver.find_element(By.XPATH, '//button[contains(@class,"login-btn")]')
        login_button.click()
        logger.info("🔐 Login submitted.")
        time.sleep(3)

        try:
            popup_chart_label = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="whatsnewModal"]/div/div/div[1]/button/span')))
            popup_chart_label.click()
        except TimeoutException:
            logger.info("No 'What's New' pop-up found, continuing.")

        logger.info("Navigating to EOD Chart Patterns page.")
        driver.get("https://intradayscreener.com/scans/eod-chart-patterns")
        time.sleep(5) # Allow page to load

        for tab_name in ["CASH", "F&O"]:
            logger.info(f"--- Processing {tab_name} tab ---")
            if tab_name == "F&O":
                fno_tab = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "F&O")]')))
                fno_tab.click()
                time.sleep(3)

            # Click on the pattern filter dropdown
            pattern_filter_button = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.ID, "pattern-filter")))
            pattern_filter_button.click()
            time.sleep(1)

            # Unselect Bearish and Neutral patterns
            for pattern_type in ["Bearish Patterns", "Neutral Patterns"]:
                checkbox = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f'//label[contains(text(), "{pattern_type}")]/preceding-sibling::input')))
                if checkbox.is_selected():
                    driver.execute_script("arguments[0].click();", checkbox)
                    logger.info(f"Unselected '{pattern_type}'.")

            # Apply the filter
            apply_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Apply")]')))
            apply_button.click()
            logger.info("Applied pattern filters.")
            time.sleep(3)


            while True:
                rows = WebDriverWait(driver, 30).until(EC.presence_of_all_elements_located((By.TAG_NAME, "mat-row")))
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "mat-cell")
                    if len(cells) >= 4:
                        all_cells_text = [cell.text for cell in cells]
                        scraped_data.append((all_cells_text, tab_name))

                try:
                    next_btn = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//button[contains(@class, "mat-mdc-paginator-navigation-next")]')))
                    if next_btn.get_attribute("disabled"):
                        logger.info(f"Reached last page for tab {tab_name}.")
                        break
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
                    driver.execute_script("arguments[0].click();", next_btn)
                    WebDriverWait(driver, 10).until(EC.staleness_of(rows[0]))
                    time.sleep(1)
                except TimeoutException:
                    logger.info(f"No more pages for tab {tab_name}.")
                    break
        return scraped_data

    except Exception as e:
        logger.error(f"An error occurred during scraping: {e}")
        return []
    finally:
        run_completed_time = get_current_ist_time_as_str()
        logger.info(f"Scraping run completed at {run_completed_time}")
        driver.quit()
        logger.info("🧹 Browser closed.")

def process_data(scraped_data):
    """
    Processes raw scraped data into a structured format for database insertion.
    """
    processed_records = []
    for all_cells_text, stock_type in scraped_data:
        symbol = all_cells_text[0]
        ltp_text = all_cells_text[1]
        date_of_formation = datetime.strptime(all_cells_text[2], '%d %b %Y').date()
        pattern_type = all_cells_text[3]

        ltp_match = re.search(r'([\d,]+\.\d+)', ltp_text)
        ltp = float(ltp_match.group(1).replace(',', '')) if ltp_match else 0.0

        percentage_change_match = re.search(r'\(([-+]?\d+\.\d+)%\)', ltp_text)
        percentage_change = float(percentage_change_match.group(1)) if percentage_change_match else 0.0

        trade_type = "BUY" if percentage_change > 0 else "SELL"

        record_dict = {
            "symbol": symbol,
            "ltp": ltp,
            "date_of_formation": date_of_formation,
            "pattern_type": pattern_type,
            "percentage_change": percentage_change,
            "trade_type": trade_type,
            "stock_type": stock_type
        }
        processed_records.append(record_dict)
    return processed_records


def write_to_db(records_to_insert, db_session):
    """
    Writes the processed records to the database.
    """
    if records_to_insert:
        logger.info(f"Starting database insertion for {len(records_to_insert)} records...")
        repo = SgEodChartPatternsRepository(db_session)
        for record in records_to_insert:
            repo.insert(record)
        logger.info("Inserted all records successfully!")
    else:
        logger.info("No new records to insert.")

def get_eod_chart_patterns(db_session):
    """
    Orchestrates the scraping, processing, and database writing process.
    """
    raw_data = run_scraper()
    if raw_data:
        processed_data = process_data(raw_data)
        write_to_db(processed_data, db_session)

# ---------------- Runner ---------------- #
if __name__ == "__main__":
    logger.info("Starting EOD Chart Patterns scraper job.")
    db_session = None
    try:
        db_session = next(get_db_session())
        get_eod_chart_patterns(db_session)
    except Exception as e:
        logger.error(f"Job failed: {e}")
    finally:
        if db_session:
            db_session.close()
    logger.info("EOD Chart Patterns scraper job finished.")
