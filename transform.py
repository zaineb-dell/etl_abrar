"""
transform.py — Transformations MMG et OCC.

Structure alignée sur les tables de production Tunisie Telecom :
  DETAIL_MMG : NE · A_MSISDN · B_MSISDN · START_DATE · START_HOUR
               EVENT_TYPE · EVENT_TYPE_ORIG · CALL_TYPE · EVENT_STATUS
               SUBSCRIBER_TYPE · SERVICE_TYPE · ORIG_START_TIME

  DETAIL_OCC : DATASOURCE · A_MSISDN · B_MSISDN · START_DATE · START_HOUR
               APN · CALL_TYPE · EVENT_TYPE · SUBSCRIBER_TYPE
               ROAMING_TYPE · PARTNER · CHARGE_AMOUNT · KEYWORD · ORIG_START_TIME

  AGG_MMG    : B_MSISDN · START_DATE · START_HOUR · CALL_TYPE · EVENT_TYPE
               EVENT_STATUS · SUBSCRIBER_TYPE · SERVICE_TYPE · CDR_COUNT

  AGG_OCC    : B_MSISDN · START_DATE · START_HOUR · CALL_TYPE · EVENT_TYPE
               SUBSCRIBER_TYPE · KEYWORD · CDR_COUNT · CHARGE_AMOUNT
"""

import pandas as pd

from config import NULL_STR, log
from db import to_date


def transform_mmg(df: pd.DataFrame, fname: str) -> pd.DataFrame:
    """Transformations MMG — produit les colonnes de DETAIL_MMG et AGG_MMG."""
    df = df.copy()

    # Étape 1 : drop colonnes 100% vides
    before_cols = len(df.columns)
    df.dropna(axis=1, how="all", inplace=True)
    log.info(f"  [MMG-1] Drop colonnes vides : {before_cols} → {len(df.columns)} col")

    # Étape 2 : sentinelles → NULL
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].where(~df[c].isin(NULL_STR), None)

    # Étape 3 : déduplication
    if "CDR_SEARCH_DETAIL_ID" in df.columns:
        before = len(df)
        df.drop_duplicates(subset=["CDR_SEARCH_DETAIL_ID"], keep="first", inplace=True)
        log.info(f"  [MMG-3] Doublons supprimés : {before - len(df):,}")

    # Filtre NE = MMG
    if "NE" in df.columns:
        df = df[df["NE"].astype(str).str.upper() == "MMG"].copy()

    # Étape 4 : parsing ORIG_START_TIME → START_DATE + START_HOUR
    src_col = (
        "ORIG_START_TIME" if "ORIG_START_TIME" in df.columns
        else ("PROC_DATE" if "PROC_DATE" in df.columns else None)
    )
    if src_col:
        df["_dt"]        = df[src_col].apply(to_date)
        df["START_DATE"] = df["_dt"].apply(lambda d: d.date() if d else None)
        df["START_HOUR"] = df["_dt"].apply(lambda d: d.hour  if d else None)
    else:
        df["_dt"]        = None
        df["START_DATE"] = None
        df["START_HOUR"] = None

    # Étape 5 : normaliser EVENT_TYPE depuis EVENT_TYPE_ORIG si absent
    if "EVENT_TYPE" not in df.columns and "EVENT_TYPE_ORIG" in df.columns:
        df["EVENT_TYPE"] = df["EVENT_TYPE_ORIG"]

    # Étape 6 : filtrage TEST_FLAG (1=test interne, 2=test externe)
    if "TEST_FLAG" in df.columns:
        before = len(df)
        df = df[~df["TEST_FLAG"].astype(str).isin(["1", "2"])].copy()
        log.info(f"  [MMG-6] TEST_FLAG exclus : {before - len(df):,} lignes retirées")

    log.info(f"  [MMG] Après transformation : {len(df):,} lignes")
    return df


def transform_occ(df: pd.DataFrame, fname: str) -> pd.DataFrame:
    """Transformations OCC — produit les colonnes de DETAIL_OCC et AGG_OCC."""
    df = df.copy()

    # Étape 1 : drop colonnes 100% vides
    # EVENT_DURATION=0 est NORMAL pour les CDR DATA — ne pas filtrer
    before_cols = len(df.columns)
    df.dropna(axis=1, how="all", inplace=True)
    log.info(f"  [OCC-1] Drop colonnes vides : {before_cols} → {len(df.columns)} col")

    # Étape 2 : sentinelles → NULL
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].where(~df[c].isin(NULL_STR), None)

    # Étape 3 : déduplication
    if "CDR_SEARCH_DETAIL_ID" in df.columns:
        before = len(df)
        df.drop_duplicates(subset=["CDR_SEARCH_DETAIL_ID"], keep="first", inplace=True)
        log.info(f"  [OCC-3] Doublons : {before - len(df):,} retirés")

    # Filtre RECORD_TYPE
    if "RECORD_TYPE" in df.columns:
        df = df[df["RECORD_TYPE"].astype(str).str.upper().isin(["OCCR", "OCC"])].copy()

    # Étape 4 : parsing ORIG_START_TIME (strip timezone +0100 géré dans to_date)
    src_col = "ORIG_START_TIME" if "ORIG_START_TIME" in df.columns else "START_TIME"
    if src_col in df.columns:
        df["_dt"]        = df[src_col].apply(to_date)
        df["START_DATE"] = df["_dt"].apply(lambda d: d.date() if d else None)
        df["START_HOUR"] = df["_dt"].apply(lambda d: d.hour  if d else None)
    else:
        df["_dt"]        = None
        df["START_DATE"] = None
        df["START_HOUR"] = None

    # Étape 5 : normaliser CHARGE_AMOUNT
    charge_col = next(
        (c for c in ["CHARGE_AMOUNT", "CHARGE_AMOUNT_ORIG"] if c in df.columns), None
    )
    if charge_col:
        df["CHARGE_AMOUNT"] = pd.to_numeric(df[charge_col], errors="coerce").fillna(0)
    else:
        df["CHARGE_AMOUNT"] = 0.0

    # Étape 6 : normaliser KEYWORD depuis SERVICE_TYPE si absent
    if "KEYWORD" not in df.columns and "SERVICE_TYPE" in df.columns:
        df["KEYWORD"] = df["SERVICE_TYPE"]

    # Étape 7 : normaliser EVENT_TYPE depuis EVENT_TYPE_ORIG si absent
    if "EVENT_TYPE" not in df.columns and "EVENT_TYPE_ORIG" in df.columns:
        df["EVENT_TYPE"] = df["EVENT_TYPE_ORIG"]

    # Étape 8 : filtrage TEST_FLAG
    if "TEST_FLAG" in df.columns:
        before = len(df)
        df = df[~df["TEST_FLAG"].astype(str).isin(["1", "2"])].copy()
        log.info(f"  [OCC-8] TEST_FLAG exclus : {before - len(df):,} retirées")

    log.info(f"  [OCC] Après transformation : {len(df):,} lignes")
    return df
