import datetime
import logging
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Boolean,
    Date,
    Time,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Assuming these modules exist based on the provided context
# In a real scenario, you would need to ensure these paths are correct
from algo_scripts.algotrade.scripts.trade_utils.time_manager import (
    get_current_ist_time_as_str,
    get_today_date_as_str,
    get_screener_run_id,
    get_ist_time,
)
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import engine, get_db_session, Base

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SG_ORB_ALERTS")

class SgOrbAlerts(Base):
    __tablename__ = "sg_orb_alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(100), default=get_screener_run_id)
    screener_date = Column(Date, default=lambda: get_ist_time()[1].date())
    screener_time = Column(Time, default=lambda: get_ist_time()[1].time())
    screener_type = Column(String(100), default="INTRADAY")
    screener = Column(String(100), default="OPENING RANGE BREAKOUT")
    trade_type = Column(String(100))
    strategy = Column(String(100))
    updated_time = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    percentage_change = Column(Float)
    change = Column(Float)
    stock_type = Column(String(100))

    # Scraped data
    symbol = Column(String(100), nullable=False)
    ltp = Column(Float)
    orb_price = Column(Float)
    deviation_from_pivots = Column(String(100))
    todays_range = Column(String(100))
    time_range = Column(Integer)

    # Additional columns
    is_prb = Column(Boolean, default=False)
    orb_time = Column(Time)
    date = Column(Date, default=lambda: get_ist_time()[1].date())

    def __repr__(self):
        return f"<SgOrbAlerts(symbol='{self.symbol}', strategy='{self.strategy}', date='{self.date}')>"


class SgOrbRepository:
    def __init__(self, session):
        self.session = session

    def insert(self, data: dict):
        """
        Inserts a new alert into the sg_orb_alerts table.
        The dictionary keys should align with the table columns.
        Relies on model defaults for values not in the data dictionary.
        """
        try:
            insert_data = {
                "symbol": data.get("symbol"),
                "ltp": data.get("ltp"),
                "orb_price": data.get("orb_price"),
                "deviation_from_pivots": data.get("deviation"),
                "todays_range": data.get("range"),
                "is_prb": data.get("is_prb_present"),
                "strategy": data.get("strategy"),
                "screener_time": get_ist_time()[1].time(),
                "orb_time": data.get("orb_time"),
                "time_range": data.get("time_range"),
                "percentage_change": data.get("percentage_change"),
                "change": data.get("change"),
                "trade_type": data.get("trade_type"),
                "stock_type": data.get("stock_type")
            }


            # Allow model to use defaults for run_id, screener_date, trade_type, updated_time, date
            alert = SgOrbAlerts(**insert_data)

            self.session.add(alert)
            self.session.commit()
            logger.info(f"Successfully inserted alert for {alert.symbol}")
            return alert
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to insert alert: {e}")
            return None

    def get_stocks_by_name_and_date(self, symbol: str, date: str):
        """
        Retrieves all alerts for a given stock symbol and date.
        Date should be in 'YYYY-MM-DD' format.
        """
        try:
            search_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
            return self.session.query(SgOrbAlerts).filter(
                SgOrbAlerts.symbol == symbol,
                SgOrbAlerts.date == search_date
            ).all()
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid date format for '{date}': {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching stocks by name and date: {e}")
            return []

    def get_stocks_by_time_range(self, time_range_as_int: int, is_prb: bool = False):
        """
        Retrieves stocks within a given time range (in minutes) and optionally filters by is_prb.
        """
        try:
            return self.session.query(SgOrbAlerts).filter(
                SgOrbAlerts.time_range == time_range_as_int,
                SgOrbAlerts.is_prb == is_prb
            ).all()
        except Exception as e:
            logger.error(f"Error fetching stocks by time range: {e}")
            return []


if __name__ == "__main__":
    # This block is for schema creation and testing purposes.
    # It will create the 'sg_orb_alerts' table if it doesn't exist.
    logger.info("Creating 'sg_orb_alerts' table if it does not exist...")
    Base.metadata.create_all(engine)
    logger.info("Table creation check complete.")

    # Example Usage (for testing)
    # with get_db_session() as session:
    #     repo = SgOrbRepository(session)
    #     # Example insert
    #     repo.insert({
    #         "symbol": "TEST", "ltp": 100.0, "orb_price": 101.0, "strategy": "ORB 15",
    #         "deviation_from_pivots": "0.5%", "todays_range": "10", "time_range": "15",
    #         "orb_time": "09:30 AM", "is_prb": False
    #     })
    #     # Example fetch
    #     stocks = repo.get_stocks_by_name_and_date("TEST", get_today_date_as_str())
    #     logger.info(f"Found stocks for TEST: {stocks}")