import os
from dotenv import load_dotenv
import logging

import pandas as pd
import tushare as ts
from openbb_tushare.utils.table_cache import TableCache
from openbb_tushare.utils.tools import setup_logger

TABLE_SCHEMA = {
    "symbol": "TEXT PRIMARY KEY",  # Unique identifier for the asset
    "ts_code": "TEXT",             # Trading symbol/code 
    "name": "TEXT",                 # Short name of the asset
    "area": "TEXT",                 # Geographic region
    "industry": "TEXT",             # Industry classification
    "fullname": "TEXT",             # Full legal name
    "enname": "TEXT",               # English name
    "cnspell": "TEXT",              # Chinese phonetic abbreviation
    "market": "TEXT",               # Market classification (e.g. A-shares)
    "exchange": "TEXT",             # Exchange code (e.g. SSE/SZSE)
    "curr_type": "TEXT",            # Currency type (CNY/USD/HKD)
    "list_status": "TEXT",          # Listing status (L/D/P)
    "list_date": "TEXT",            # Listing date (YYYYMMDD)
    "delist_date": "TEXT",          # Delisting date (YYYYMMDD)
    "is_hs": "TEXT",                # Hong Kong Connect eligibility (N/H/S)
    "act_name": "TEXT",             # Actual controller name
    "act_ent_type": "TEXT",         # Controller entity type
}

setup_logger()
logger = logging.getLogger(__name__)

def get_available_indices(use_cache: bool = True, api_key : str = "") -> pd.DataFrame:
    if api_key:
        tushare_api_key = api_key
    else:
        load_dotenv() 
        tushare_api_key = os.environ.get("TUSHARE_API_KEY")
    if tushare_api_key is None:
        raise ValueError("TUSHARE_API_KEY environment variable not set.")

    cache = TableCache(TABLE_SCHEMA, table_name="indices")
    if use_cache:
        data = cache.read_dataframe()
        if not data.empty:
            logger.info("Loading indices from cache...")
            return data

    logger.info(f"Generating new indices data...")
    pro = ts.pro_api(tushare_api_key)
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type')
    cache.write_dataframe(data)
    return data