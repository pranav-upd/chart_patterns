import logging
import os
import time
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import datetime
import csv

# ---------------- Load Env ---------------- #
load_dotenv()

# ---------------- Setup Logging ---------------- #
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("GET_CHART_PATTERNS")

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
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
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

        logger.info("Navigating to EOD Scans page.")
        eod_scans_menu = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '//a[contains(text(), "EOD Scans")]'))
        )
        eod_scans_menu.click()
        time.sleep(1)
        chart_patterns_link = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '//a[contains(text(), "Chart Patterns")]'))
        )
        chart_patterns_link.click()
        logger.info("🎨 Navigated to Chart Patterns page.")
        time.sleep(3)

        # Unselect Bearish and Neutral Patterns
        logger.info("Deselecting Bearish and Neutral patterns...")
        for pattern_type in ["Bearish Patterns", "Neutral Patterns"]:
            checkbox = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, f'//label[contains(text(), "{pattern_type}")]/preceding-sibling::input[@type="checkbox"]'))
            )
            if checkbox.is_selected():
                driver.execute_script("arguments[0].click();", checkbox)
                logger.info(f"Unselected '{pattern_type}'.")

        apply_button = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Apply")]'))
        )
        apply_button.click()
        logger.info("Applied filters.")
        time.sleep(3)


        # Scrape from Cash and F&O tabs
        tab_xpaths = [
            ('//button[contains(text(), "Cash")]', "CASH"),
            ('//button[contains(text(), "F&O")]', "FNO")
        ]

        for tab_xpath, tab_name in tab_xpaths:
            logger.info(f"Processing tab: {tab_name}")
            tab_element = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, tab_xpath)))
            driver.execute_script("arguments[0].click();", tab_element)
            time.sleep(2)

            # Scraping logic for each tab
            while True:
                rows = WebDriverWait(driver, 30).until(EC.presence_of_all_elements_located((By.TAG_NAME, "mat-row")))
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "mat-cell")
                    # Extract Symbol, LTP, Date of Formation, Pattern Type
                    # Based on the prompt, the indices should be:
                    # 0: Symbol, 1: LTP, 2: Date of Formation, 3: Pattern Type
                    row_data = [cells[i].text for i in [0, 1, 2, 3]]
                    scraped_data.append((row_data, tab_name))

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
        return [] # Return empty list on failure
    finally:
        logger.info(f"Scraping run completed.")
        driver.quit()
        logger.info("🧹 Browser closed.")


def process_data(scraped_data):
    """
    Processes raw scraped data into a structured format for CSV writing.
    Args:
        scraped_data (list): Raw data from the scraper.
    Returns:
        list: A list of dictionaries, with each dictionary representing a record.
    """
    processed_records = []
    for row_data, tab_name in scraped_data:
        symbol, ltp_str, date_of_formation, pattern_type = row_data

        # Parse LTP string like "1,234.56 (+1.23%)"
        ltp_parts = ltp_str.replace(',', '').split()
        ltp = float(ltp_parts[0])
        percentage_change_str = ltp_parts[1].strip('()%')
        percentage_change = float(percentage_change_str)

        trade_type = "BUY" if percentage_change > 0 else "SELL"

        record = {
            "symbol": symbol,
            "ltp": ltp,
            "percentage_change": percentage_change,
            "date_of_formation": date_of_formation,
            "pattern_type": pattern_type,
            "trade_type": trade_type,
            "source_tab": tab_name
        }
        processed_records.append(record)
    return processed_records


def write_to_csv(records_to_insert):
    """
    Writes the processed records to a CSV file.
    Args:
        records_to_insert (list): A list of dictionaries to insert.
    """
    if not records_to_insert:
        logger.info("No new records to write.")
        return

    keys = records_to_insert[0].keys()
    with open('chart_patterns.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(records_to_insert)
    logger.info("Wrote records to chart_patterns.csv")


def get_chart_pattern_alerts():
    """
    Orchestrates the scraping, processing, and CSV writing process.
    """
    raw_data = run_scraper()
    if raw_data:
        processed_data = process_data(raw_data)
        write_to_csv(processed_data)


# ---------------- Runner ---------------- #
if __name__ == "__main__":
    logger.info("Starting Chart Patterns alerts scraper job.")
    try:
        get_chart_pattern_alerts()
    except Exception as e:
        logger.error(f"Job failed: {e}")
    logger.info("Chart Patterns alerts scraper job finished.")
