import datetime
import logging
from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Date,
    Time,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Assuming these modules exist based on the provided context
# In a real scenario, you would need to ensure these paths are correct
from algo_scripts.algotrade.scripts.trade_utils.time_manager import (
    get_ist_time,
    get_screener_run_id
)
from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import Base, engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SG_EOD_CHART_PATTERNS")

class SgEodChartPatterns(Base):
    __tablename__ = "sg_eod_chart_patterns"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(100), default=get_screener_run_id)
    screener_date = Column(Date, default=lambda: get_ist_time()[1].date())
    screener_time = Column(Time, default=lambda: get_ist_time()[1].time())
    screener_type = Column(String(100), default="EOD")
    screener = Column(String(100), default="CHART PATTERNS")

    symbol = Column(String(100), nullable=False)
    ltp = Column(Float)
    date_of_formation = Column(Date)
    pattern_type = Column(String(100))
    percentage_change = Column(Float)
    trade_type = Column(String(50))
    stock_type = Column(String(50)) # F&O or Cash
    updated_time = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    def __repr__(self):
        return f"<SgEodChartPatterns(symbol='{self.symbol}', pattern='{self.pattern_type}', date='{self.screener_date}')>"

class SgEodChartPatternsRepository:
    def __init__(self, session):
        self.session = session

    def insert(self, data: dict):
        """
        Inserts a new chart pattern alert into the sg_eod_chart_patterns table.
        """
        try:
            pattern = SgEodChartPatterns(
                symbol=data.get("symbol"),
                ltp=data.get("ltp"),
                date_of_formation=data.get("date_of_formation"),
                pattern_type=data.get("pattern_type"),
                percentage_change=data.get("percentage_change"),
                trade_type=data.get("trade_type"),
                stock_type=data.get("stock_type")
            )
            self.session.add(pattern)
            self.session.commit()
            logger.info(f"Successfully inserted EOD chart pattern for {pattern.symbol}")
            return pattern
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to insert EOD chart pattern: {e}")
            return None

    def get_chart_pattern_by_screener(self, screener_name: str):
        """
        Retrieves all alerts for a given screener name.
        """
        try:
            return self.session.query(SgEodChartPatterns).filter(
                SgEodChartPatterns.screener == screener_name
            ).all()
        except Exception as e:
            logger.error(f"Error fetching chart patterns by screener: {e}")
            return []

    def get_chart_pattern_by_screener_and_date(self, screener_name: str, date: str):
        """
        Retrieves all alerts for a given screener name and date.
        Date should be in 'YYYY-MM-DD' format.
        """
        try:
            search_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
            return self.session.query(SgEodChartPatterns).filter(
                SgEodChartPatterns.screener == screener_name,
                SgEodChartPatterns.screener_date == search_date
            ).all()
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid date format for '{date}': {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching chart patterns by screener and date: {e}")
            return []

    def get_chart_pattern_by_screener_by_pattern_type(self, screener_name: str, pattern_type: str):
        """
        Retrieves all alerts for a given screener name and pattern type.
        """
        try:
            return self.session.query(SgEodChartPatterns).filter(
                SgEodChartPatterns.screener == screener_name,
                SgEodChartPatterns.pattern_type == pattern_type
            ).all()
        except Exception as e:
            logger.error(f"Error fetching chart patterns by screener and pattern type: {e}")
            return []

if __name__ == "__main__":
    logger.info("Creating 'sg_eod_chart_patterns' table if it does not exist...")
    Base.metadata.create_all(engine)
    logger.info("Table creation check complete.")
