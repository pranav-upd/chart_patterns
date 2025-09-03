

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel
import logging
from dotenv import load_dotenv

from datetime import datetime
import pytz

from algo_scripts.algotrade.scripts.fyers.check_fyers_token import check_fyers_token
from algo_scripts.algotrade.scripts.trade_utils.trade_logger import get_trade_actions_dynamic_logger, \
    get_buy_logger_name

from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.db_logger import log_tv_signal

from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import (
    get_db_session, initialize_global_session, cleanup,close_global_session
)


import os
import atexit
import json




"""
Fyers	
Stock  - NSE:SBIN-EQ	
Future - NSE:RELIANCE25MARFUT	
Option Weekly - NSE:NIFTYYYMDD19000CE	 Example  NSE:NIFTY2521323500PE	
Option Monthly - NSE:NIFTY25FEB23500CE	
Commodity -  MCX:CRUDEOIL24MARFUT	
Currency - NSE:USDINR24MARFUT	

https://algotradingbridge.in/support/broker-master-script-symbol-instrument-contract-list/

NIFTY1! – This also represents the nearest expiry contract (current month).
NIFTY2! – This represents the next month’s contract.
NIFTYG2025, NIFTYH2025, NIFTYJ2025 – These are far-month futures (February, March, April 2025)
"""


load_dotenv(override=True)
SOURCE_REPO = os.getenv('SOURCE_REPO')
HOTT_BUY_URL= os.getenv('HOTT_BUY_URL')

# Basic logging configuration for fallback logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (change this in production)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

# Register FastAPI startup event
@app.on_event("startup")
def startup():
    """Initialize global session at startup."""
    initialize_global_session()

# Register FastAPI shutdown event
@app.on_event("shutdown")
def shutdown():
    """Cleanup database connections when the app shuts down."""
    close_global_session()
    cleanup()

# Also register cleanup in case the container is forcefully stopped
atexit.register(cleanup)

class TradingViewSignalPayload(BaseModel):
    exchange: str
    ticker: str
    trade_type: str
    order_type: str
    quantity: int
    limitprice: float
    time: str
    strategy: str
    interval: str
    alertName: str
    open:float
    close:float
    high:float
    low:float

class TradingViewFNOPayload(BaseModel):
    exchange: str
    ticker: str
    trade_type: str
    order_type: str
    quantity: int
    limitprice: float
    time: str
    strategy: str
    interval: str
    alertName: str
    open:float
    close:float
    high:float
    low:float


class TradingViewBuyHOTTPayload(BaseModel):
    exchange: str
    ticker: str
    trade_type: str
    order_type: str
    quantity: int
    limitprice: float
    time: str
    strategy: str
    interval: str
    alertName: str
    open: float
    close: float
    high: float
    low: float

    # Derived from plots
    buy_signal_strategy: int                       # plot_0 (buySignalRank → int)
    golden_cross: int                              # plot_1 (boolean → 0/1)
    valid_hott_crossover_signal: int                      # plot_2 (boolean → 0/1)
    atr: float                                     # plot_3 (Average True Range → float)
    utbot_buy: int                                 # plot_4 (boolean → 0/1)
    valid_utbuy_candle: int                        # plot_5 (boolean → 0/1)
    valid_signal_candle: int                       # plot_6 (boolean → 0/1)
    is_high_volume: int                            # plot_7 (boolean → 0/1)
    ema_vwap_crossover: int                        # plot_8 (boolean → 0/1)
    ema_hott_crossover: int                        # plot_9 (boolean → 0/1)
    triple_crossover: int                          # plot_10 (boolean → 0/1)
    resistance_price: float                        # plot_11
    resistance_breakout: int                       # plot_12 (boolean → 0/1)
    next_resistance_price: float                   # plot_13
    enough_room_to_next_resistance: int            # plot_14 (boolean → 0/1)
    latest_hott_value: float                       # plot_15
    breakout_flags: int                            # plot_16 (bitmask flags → int)
    breakout_count: int                            # plot_17 (number of breakouts → int)

def convert_to_ist(utc_time_str):
    try:
        utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ")
        utc_zone = pytz.timezone("UTC")
        ist_zone = pytz.timezone("Asia/Kolkata")
        utc_time = utc_zone.localize(utc_time)
        ist_time = utc_time.astimezone(ist_zone)
        return ist_time.strftime("%Y-%m-%d %I:%M:%S %p %Z")
    except Exception:
        return f"UTC: {utc_time_str}"

# Endpoint to trigger a limit order action
@app.post("/signal_data/loader")
async def load_signal_data(payload: TradingViewSignalPayload, db: Session = Depends(get_db_session)):
    logger_prefix = "load_signal_"
    process_logger_name =get_buy_logger_name(logger_prefix,payload)
    logger = get_trade_actions_dynamic_logger(process_logger_name)  # Dynamic logger
    row_id = -1
    try:
        logger.info(f" SLV processing: {payload.ticker}")
        logger.info(f" SLV Algo Received signal payload for processing: {payload}")

        alert_message = [
            {
                "stock_name": payload.ticker,
                "order_response": "Received trading signal processing"
            }
        ]

        try:
            payload.time = convert_to_ist(payload.time)
        except Exception as e:
            logger.warning(f"Failed to convert time: {e}")
            payload.time = "UTC: " + payload.time

        order_details = [str(value) for value in dict(payload).values()] + [json.dumps(alert_message)]

        # TODO: Refactor log_tv_signal in Step 3 to ensure it uses the provided session
        row_id = log_tv_signal(order_details, db, process_logger_name)

        logger.info(f"row_id : {row_id} Created")
        return {"status": "Success", "message": f"Row ID: {str(row_id)} inserted"}

    except Exception as e:
        logger.error(f"Failed to process signal: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to process signal: {str(e)}")

@app.get("/check_signal_processor_token/v1/")
async def check_valid_token():
    logger_name = "signal_processor_check_token"
    response = await check_fyers_token(logger_name)  # Await the coroutine
    return response



@app.get("/signal_data_loader/health")
def health_check():
    return {"status": "Signal_Data_Loader healthy"}

