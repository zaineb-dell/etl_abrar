"""
config.py — Configuration centrale de l'ETL Télécom
Toutes les variables d'environnement, constantes métier et logger.
"""

import io
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Oracle ─────────────────────────────────────────────────────
ORACLE_INSTANT_CLIENT = os.getenv("ORACLE_INSTANT_CLIENT", r"C:\instantclient_23_0")
ORACLE_CONFIG = {
    "user":         os.getenv("DB_USER",     "DW_TELECOM"),
    "password":     os.getenv("DB_PASSWORD", "Abrar321"),
    "host":         os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT",     "1521")),
    "service_name": os.getenv("DB_SERVICE",  "XEPDB1"),
}
ORACLE_SCHEMA = os.getenv("DB_SCHEMA", "DW_TELECOM")
COMMIT_EVERY  = int(os.getenv("COMMIT_EVERY", "5000"))

# ── Scheduler ──────────────────────────────────────────────────
SCHEDULE_HEURE  = int(os.getenv("SCHEDULE_HEURE",  "2"))
SCHEDULE_MINUTE = int(os.getenv("SCHEDULE_MINUTE", "0"))

# ── Répertoires ────────────────────────────────────────────────
INPUT_DIR   = Path(os.getenv("INPUT_DIR",   "./data/input"))
ARCHIVE_DIR = Path(os.getenv("ARCHIVE_DIR", "./data/archive"))
LOG_DIR     = Path(os.getenv("LOG_DIR",     "./logs"))

# ── FTP / SFTP ─────────────────────────────────────────────────
FTP_ENABLED    = os.getenv("FTP_ENABLED",    "false").lower() == "true"
FTP_PROTOCOL   = os.getenv("FTP_PROTOCOL",   "ftp").lower()
FTP_HOST       = os.getenv("FTP_HOST",       "")
FTP_PORT       = int(os.getenv("FTP_PORT",   "21"))
FTP_USER       = os.getenv("FTP_USER",       "")
FTP_PASSWORD   = os.getenv("FTP_PASSWORD",   "")
FTP_KEY_PATH   = os.getenv("FTP_KEY_PATH",   "")
FTP_REMOTE_DIR = os.getenv("FTP_REMOTE_DIR", "/exports")
FTP_EXTENSIONS = {e.strip() for e in os.getenv("FTP_EXTENSIONS", ".csv,.xlsx").split(",")}
FTP_MIN_SIZE   = int(os.getenv("FTP_MIN_SIZE",  "10"))
FTP_MAX_RETRY  = int(os.getenv("FTP_MAX_RETRY", "3"))
FTP_RETRY_WAIT = int(os.getenv("FTP_RETRY_WAIT","5"))
FTP_REGISTRY   = Path(os.getenv("FTP_REGISTRY", "./data/.ftp_downloaded.txt"))

# ── Constantes métier ──────────────────────────────────────────
NULL_STR    = {"_N", "_UN", "nan", "NaN", "NULL", "None", ""}
SVC_UNKNOWN = "_UNKNOWN_"   # Sentinel pour NULL dans DIM_SERVICE (UNIQUE Oracle)

BEARER_LABELS = {
    "21": "GPRS", "22": "EDGE", "23": "3G",
    "101": "LTE", "102": "3G", "103": "LTE",
    "104": "LTE-A", "116": "5G",
}

REQUIRED_COLS = {
    "MMG": {"A_MSISDN", "PROC_DATE"},
    "OCC": {"A_MSISDN", "START_TIME"},
}

MONTH_FR = {
    1:"Janvier", 2:"Février",  3:"Mars",      4:"Avril",
    5:"Mai",     6:"Juin",     7:"Juillet",   8:"Août",
    9:"Septembre", 10:"Octobre", 11:"Novembre", 12:"Décembre",
}
DAY_FR = {
    0:"Lundi", 1:"Mardi", 2:"Mercredi", 3:"Jeudi",
    4:"Vendredi", 5:"Samedi", 6:"Dimanche",
}

# ── Logger ─────────────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_DIR / f"etl_{datetime.now().strftime('%Y%m%d')}.log",
            encoding="utf-8",
        ),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("ETL_TELECOM")
