import psycopg2
from dotenv import load_dotenv
import os

# ------------------------
# API Configuration - Padel Web
# ------------------------
API_BASE_URL = "https://padelandwin.cat/ajax/ajax.aspx/"
API_HEADERS = {
    "Content-Type": "application/json; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://padelandwin.cat/torneo/?t=MzE5",
    "Origin": "https://padelandwin.cat",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
API_COOKIES = {
    "PadelWinCookie": "user=d/XvBfH7jw0lIYJOpAWmiw==&cookies=MA==&date=MTUvMDkvMjAyNSAxNzo0MDo1NQ=="
}

# ------------------------
# Configuración Supabase / PostgreSQL
# ------------------------
load_dotenv()  # load env variables
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
DBNAME = os.getenv("DB_NAME")