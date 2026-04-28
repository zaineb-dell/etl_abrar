"""
extract.py — Extraction locale et détection du type CDR (MMG / OCC).
"""

from pathlib import Path

import pandas as pd

from config import NULL_STR, REQUIRED_COLS, log


# ── Listes des colonnes brutes à conserver par type ────────────

RAW_MMG_COLS = [
    "CDR_SEARCH_DETAIL_ID", "NE", "PROC_DATE", "PROC_HOUR", "A_MSISDN", "B_MSISDN",
    "SUBSCRIBER_TYPE", "SERVICE_TYPE", "EVENT_TYPE_ORIG", "EVENT_STATUS",
    "CHARGE_AMOUNT_ORIG", "PARTNER_CODE", "PARTNER", "ORIG_START_TIME",
    "TEST_FLAG", "FILTER_CODE", "PORTABILITY_FLAG", "IMEI", "PARTIAL_SEQ_ID",
]

RAW_OCC_COLS = [
    "CDR_SEARCH_DETAIL_ID", "RECORD_TYPE", "NE", "A_MSISDN", "B_MSISDN",
    "SUBSCRIBER_TYPE", "ORIG_START_TIME", "START_TIME", "EVENT_DURATION",
    "DATA_VOLUME", "DATA_VOLUME_UP", "DATA_VOLUME_DOWN", "CHARGE_AMOUNT_ORIG",
    "SERVICE_TYPE", "PARTNER_CODE", "PARTNER", "BEARER_SERVICE", "CALL_REFERENCE",
    "PARTIAL_SEQ_ID", "TEST_FLAG", "CAUSE_FOR_CLOSING", "SERVICE_ID",
    "FILTER_CODE", "IMEI",
]


def extract_local(path: Path) -> pd.DataFrame:
    """Lit un fichier CSV ou Excel et remplace les sentinelles par None."""
    log.info(f"Extraction : {path.name}")
    s = path.suffix.lower()
    if s == ".csv":
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False, dtype=str)
    elif s in (".xlsx", ".xls"):
        df = pd.read_excel(path, engine="openpyxl", dtype=str)
    else:
        raise ValueError(f"Format non supporté : {s}")
    df.replace(list(NULL_STR - {""}), None, inplace=True)
    log.info(f"  {len(df):,} lignes lues — {len(df.columns)} colonnes")
    return df


def detect_type(df: pd.DataFrame) -> str:
    """Détecte si le DataFrame est de type MMG ou OCC."""
    if "NE" in df.columns:
        vals = df["NE"].dropna().astype(str).str.upper().unique()
        if "MMG" in vals:
            return "MMG"
        if any("TTOCC" in v or v == "OCC" for v in vals):
            return "OCC"
    if "RECORD_TYPE" in df.columns:
        rt = df["RECORD_TYPE"].dropna().astype(str).str.upper().unique()
        if "PSM" in rt:
            return "MMG"
        if any(v in ("OCCR", "OCC") for v in rt):
            return "OCC"
    if "START_TIME" in df.columns and "DATA_VOLUME" in df.columns:
        return "OCC"
    if "PROC_DATE" in df.columns:
        return "MMG"
    raise ValueError(
        f"Type MMG/OCC non détectable — colonnes : {list(df.columns)}"
    )


def validate_schema(df: pd.DataFrame, dtype: str) -> None:
    """Lève une erreur si des colonnes obligatoires sont manquantes."""
    missing = REQUIRED_COLS.get(dtype, set()) - set(df.columns)
    if missing:
        raise ValueError(f"[{dtype}] Colonnes manquantes : {missing}")
