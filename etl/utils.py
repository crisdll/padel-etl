import requests
import pandas as pd
import re
import json
import base64
import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import API_BASE_URL, API_HEADERS, API_COOKIES

logger = logging.getLogger('padel_etl')


def convert_to_base64(value): 
    return base64.b64encode(str(value).encode()).decode()

def convert_to_dataframe(data):
    return pd.DataFrame(json.loads(data['d']))

def clean_string(string):    
    match = re.search(r'>\s*([^<]+)\s*<', string)
    if match:
        return match.group(1).strip()
    return string

def extract_id_partido(verpartidos):
    match = re.search(r"this,'(\d+)'", verpartidos)
    return match.group(1) if match else None

def convert_to_datetime(date_string, format='%d/%m/%Y %H:%M'):
    try:
        return pd.to_datetime(date_string, format=format)
    except ValueError:
        return pd.NaT
    
def convert_to_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def get_data(endpoint, payload):
    url = API_BASE_URL + endpoint
    
    logger.debug(f"API Request: {endpoint} | Payload: {payload}")
    
    try:
        response = requests.post(url, headers=API_HEADERS, cookies=API_COOKIES, json=payload)
        
        logger.debug(f"API Response: {response.status_code} | Content-Type: {response.headers.get('content-type', 'N/A')}")
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Error HTTP {response.status_code} en {endpoint}")
            logger.debug(f"Response: {response.text[:500]}")
            return None
            
    except requests.exceptions.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        logger.debug(f"Response text: {response.text[:500]}")
        return None
    except Exception as e:
        logger.error(f"Request error: {e}")
        return None