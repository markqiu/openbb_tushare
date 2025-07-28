import os
from dotenv import load_dotenv

def get_api_key(api_key : str = "") -> str:
    if api_key:
        tushare_api_key = api_key
    else:
        load_dotenv() 
        tushare_api_key = os.environ.get("TUSHARE_API_KEY")
    if tushare_api_key is None:
        raise ValueError("TUSHARE_API_KEY environment variable not set.")

    return tushare_api_key