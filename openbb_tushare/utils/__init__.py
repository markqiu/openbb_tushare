"""Tushare utils directory."""
import os
from typing import Tuple
from openbb_core.app.utils import get_user_cache_directory

def get_log_path() -> str:
    """
    Get the path for Tushare log file.

    Returns:
        str: The path to the Tushare log file.
    """
    log_dir = f"{get_user_cache_directory()}/tushare"
    log_path = f"{log_dir}/openbb_tushare.log"

    os.makedirs(log_dir, exist_ok=True)

    return log_path

def get_cache_path() -> str:
    """
    Get the path for Tushare cache database.

    Returns:
        str: The path to the Tushare cache database.
    """
    db_dir = f"{get_user_cache_directory()}/tushare"
    db_path = f"{db_dir}/equity.db"

    os.makedirs(db_dir, exist_ok=True)

    return db_path