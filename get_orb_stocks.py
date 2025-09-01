import logging
import os
import time
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import datetime

from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.signals.sg_orb_screener import SgOrbRepository
from algo_scripts.algotrade.scripts.trade_utils.time_manager import (
    get_current_ist_time_as_str,
    get_screener_run_id,
    get_ist_time,
)
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import get_db_session

# ---------------- Load Env ---------------- #
load_dotenv()

# ---------------- Setup Logging ---------------- #
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("GET_ORB_ALERTS")

INTRADAY_SCREENER_EMAIL = os.getenv("INTRADAY_SCREENER_EMAIL")
INTRADAY_SCREENER_PWD = os.getenv("INTRADAY_SCREENER_PWD")


# ---------------- Refactored Functions ---------------- #

def run_scraper():
    """
    Handles all Selenium browser interactions: setup, login, navigation, and scraping.
    Returns:
        list: A list of tuples, where each tuple contains the raw text of a table row
              and the strategy name (e.g., "ORB+PRB 15").
    """
    logger.info("🚀 Launching browser...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    driver.set_window_size(1920, 1080)
    scraped_data = []

    try:
        driver.get("https://intradayscreener.com/login")
        logger.info("🌐 Opened login page.")

        email_field = WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH, '//input[@type="email"]'))
        )
        email_field.send_keys(INTRADAY_SCREENER_EMAIL)
        password_field = driver.find_element(By.XPATH, '//input[@type="password"]')
        password_field.send_keys(INTRADAY_SCREENER_PWD)
        login_button = driver.find_element(By.XPATH, '//button[contains(@class,"login-btn")]')
        login_button.click()
        logger.info("🔐 Login submitted.")
        time.sleep(3)

        try:
            popup_chart_label = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="whatsnewModal"]/div/div/div[1]/button/span'))
            )
            popup_chart_label.click()
        except TimeoutException:
            logger.info("No 'What's New' pop-up found, continuing.")

        logger.info("Navigating to ORB page.")
        driver.get("https://intradayscreener.com/opening-range-breakout")
        time.sleep(2)

        tab_xpaths = [
            ('//*[@id="pills-home-15min"]', "ORB+PRB 15"), ('//*[@id="pills-home-15minp"]', "ORB 15"),
            ('//*[@id="pills-home-30min"]', "ORB+PRB 30"), ('//*[@id="pills-home-30minp"]', "ORB 30"),
            ('//*[@id="pills-home-45min"]', "ORB+PRB 45"), ('//*[@id="pills-home-45minp"]', "ORB 45"),
            ('//*[@id="pills-home-60min"]', "ORB+PRB 60"), ('//*[@id="pills-home-60minp"]', "ORB 60"),
        ]

        for tab_xpath, option in tab_xpaths:
            logger.info(f"Processing tab: {option}")
            tab_element = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, tab_xpath)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tab_element)
            driver.execute_script("arguments[0].click();", tab_element)
            time.sleep(2)

            while True:
                rows = WebDriverWait(driver, 30).until(EC.presence_of_all_elements_located((By.TAG_NAME, "mat-row")))
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "mat-cell")
                    all_cells_text = [cell.text for cell in cells]
                    scraped_data.append((all_cells_text, option))

                try:
                    next_btn = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//button[contains(@class, "mat-mdc-paginator-navigation-next")]')))
                    if next_btn.get_attribute("disabled"):
                        logger.info(f"Reached last page for tab {option}.")
                        break
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
                    driver.execute_script("arguments[0].click();", next_btn)
                    WebDriverWait(driver, 10).until(EC.staleness_of(rows[0]))
                    time.sleep(1)
                except TimeoutException:
                    logger.info(f"No more pages for tab {option}.")
                    break
        return scraped_data
    except Exception as e:
        logger.error(f"An error occurred during scraping: {e}")
        return [] # Return empty list on failure
    finally:
        run_completed_time = get_current_ist_time_as_str()
        logger.info(f"Scraping run completed at {run_completed_time}")
        driver.quit()
        logger.info("🧹 Browser closed.")


def process_data(scraped_data):
    """
    Processes raw scraped data into a structured format for database insertion.
    Args:
        scraped_data (list): Raw data from the scraper.
    Returns:
        list: A list of dictionaries, with each dictionary representing a record.
    """
    processed_records = []
    symbols_seen = set()
    run_id = get_screener_run_id()
    date_time = get_ist_time()[1]

    for all_cells_text, option in scraped_data:
        symbols_seen.add(all_cells_text[0])
        record_dict = {
            "symbol": all_cells_text[0], "ltp": float(all_cells_text[1].split()[0]), "orb_price": float(all_cells_text[2]),
            "orb_time": datetime.datetime.strptime(all_cells_text[3], "%I:%M %p").time(), "deviation": all_cells_text[4], "range": all_cells_text[5],
            "percentage_change":float(all_cells_text[1].split()[2][1:]), "change":float(all_cells_text[1].split()[1]), "time_range":int(option[-2:])
        }
        record_dict.update({
            "run_id": run_id, "last_updated": date_time, "stock_type": "CASH", "strategy": "ORB",

            "trade_type": "BUY" if float(all_cells_text[1].split()[2][1:])>0 else "SELL", "time_range_orb": option[-2:], "screener": "INTRADAY ORB",
            "is_prb_present": "PRB" in option,
        })
        processed_records.append(record_dict)
    return processed_records


def write_to_db(records_to_insert, db_session):
    """
    Writes the processed records to the database.
    Args:
        records_to_insert (list): A list of dictionaries to insert.
        db_session: The database session object.
    """
    if records_to_insert:
        logger.info(f"Starting database insertion for {len(records_to_insert)} records...")
        repo = SgOrbRepository(db_session)
        for record in records_to_insert:
            repo.insert(record)
        logger.info("Inserted all records successfully!")
    else:
        logger.info("No new records to insert.")


def get_orb_alerts(db_session):
    """
    Orchestrates the scraping, processing, and database writing process.
    """
    raw_data = run_scraper()
    if raw_data:
        processed_data = process_data(raw_data)
        write_to_db(processed_data, db_session)


# ---------------- Runner ---------------- #
if __name__ == "__main__":
    logger.info("Starting ORB alerts scraper job.")
    db_session = None
    try:
        db_session = next(get_db_session())
        get_orb_alerts(db_session)
    except Exception as e:
        logger.error(f"Job failed: {e}")
    finally:
        if db_session:
            db_session.close()
    logger.info("ORB alerts scraper job finished.")