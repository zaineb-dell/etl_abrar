"""
load.py — Chargement des niveaux 1, 2 et 3 (RAW, DETAIL, AGG).

  insert_raw()                  → Niveau 1 : copie brute du fichier
  insert_detail()               → Niveau 2 : données transformées
  aggregate_and_insert_mmg()    → Niveau 3 : agrégation SMS par abonné
  aggregate_and_insert_occ()    → Niveau 3 : agrégation DATA par abonné
"""

from datetime import datetime

import pandas as pd

from config import NULL_STR, log
from db import tbl, batch_execute, to_none
from extract import RAW_MMG_COLS, RAW_OCC_COLS


# ── Niveau 1 : RAW ─────────────────────────────────────────────

def insert_raw(df_raw: pd.DataFrame, dtype: str, fname: str, conn) -> int:
    """Insère une copie exacte des colonnes brutes dans RAW_MMG ou RAW_OCC."""
    raw_table = tbl(f"RAW_{dtype}")
    cols_wanted = RAW_MMG_COLS if dtype == "MMG" else RAW_OCC_COLS
    cols_present = [c for c in cols_wanted if c in df_raw.columns]

    sub = df_raw[cols_present].copy()
    sub.insert(0, "fichier_source", fname)

    col_list = ["fichier_source"] + cols_present
    placeholders = ",".join(f":{i+1}" for i in range(len(col_list)))
    sql = f"INSERT INTO {raw_table} ({','.join(col_list)}) VALUES ({placeholders})"

    # Vectorisé — zip() au lieu de iterrows()
    rows = list(zip(*[
        sub[c].apply(lambda v: str(v) if v is not None and str(v) not in NULL_STR else None)
        for c in col_list
    ]))

    cur = conn.cursor()
    n = batch_execute(cur, conn, sql, rows)
    cur.close()
    log.info(f"  RAW_{dtype} : {n:,} lignes insérées")
    return n


# ── Niveau 2 : DETAIL ──────────────────────────────────────────

def insert_detail(df: pd.DataFrame, dtype: str, fname: str, conn) -> int:
    """Insère les données transformées dans DETAIL_MMG ou DETAIL_OCC."""
    now = datetime.now()
    cur = conn.cursor()

    if dtype == "MMG":
        rows = []
        for _, r in df.iterrows():
            rows.append((
                fname, now,
                to_none(r.get("NE")),
                to_none(r.get("A_MSISDN")),
                to_none(r.get("B_MSISDN")),
                r.get("START_DATE"),
                r.get("START_HOUR"),
                to_none(r.get("EVENT_TYPE")),
                to_none(r.get("EVENT_TYPE_ORIG")),
                to_none(r.get("CALL_TYPE")),
                to_none(r.get("EVENT_STATUS")),
                to_none(r.get("SUBSCRIBER_TYPE")),
                to_none(r.get("SERVICE_TYPE")),
                to_none(r.get("ORIG_START_TIME")),
            ))
        sql = f"""INSERT INTO {tbl('DETAIL_MMG')}
            (fichier_source,etl_chargement,NE,A_MSISDN,B_MSISDN,
             START_DATE,START_HOUR,EVENT_TYPE,EVENT_TYPE_ORIG,CALL_TYPE,
             EVENT_STATUS,SUBSCRIBER_TYPE,SERVICE_TYPE,ORIG_START_TIME)
            VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)"""

    else:  # OCC
        rows = []
        for _, r in df.iterrows():
            rows.append((
                fname, now,
                to_none(r.get("NE")),
                to_none(r.get("A_MSISDN")),
                to_none(r.get("B_MSISDN")),
                r.get("START_DATE"),
                r.get("START_HOUR"),
                to_none(r.get("APN")),
                to_none(r.get("CALL_TYPE")),
                to_none(r.get("EVENT_TYPE_ORIG") or r.get("EVENT_TYPE")),
                to_none(r.get("SUBSCRIBER_TYPE")),
                to_none(r.get("ROAMING_TYPE")),
                to_none(r.get("PARTNER") or r.get("PARTNER_CODE")),
                round(float(r.get("CHARGE_AMOUNT_ORIG") or r.get("CHARGE_AMOUNT") or 0), 5),
                to_none(r.get("KEYWORD") or r.get("SERVICE_TYPE")),
                to_none(r.get("ORIG_START_TIME") or r.get("START_TIME")),
            ))
        sql = f"""INSERT INTO {tbl('DETAIL_OCC')}
            (fichier_source,etl_chargement,DATASOURCE,A_MSISDN,B_MSISDN,
             START_DATE,START_HOUR,APN,CALL_TYPE,EVENT_TYPE,
             SUBSCRIBER_TYPE,ROAMING_TYPE,PARTNER,CHARGE_AMOUNT,KEYWORD,ORIG_START_TIME)
            VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16)"""

    n = batch_execute(cur, conn, sql, rows)
    cur.close()
    log.info(f"  DETAIL_{dtype} : {n:,} lignes insérées")
    return n


# ── Niveau 3 : AGG ─────────────────────────────────────────────



def aggregate_and_insert_mmg(df, fname, conn):
    """Agrège les SMS MMG par B_MSISDN + date + heure + service → MERGE AGG_MMG."""
    now = datetime.now()
    if df.empty or "B_MSISDN" not in df.columns: return 0
    df = df.copy()

    gk = [c for c in ["B_MSISDN","START_DATE","START_HOUR","CALL_TYPE",
                       "EVENT_TYPE","EVENT_STATUS","SUBSCRIBER_TYPE","SERVICE_TYPE"]
          if c in df.columns]
    agg = df.groupby(gk, dropna=False).agg(CDR_COUNT=("B_MSISDN","count")).reset_index()

    sql = f"""
        MERGE INTO {tbl('AGG_MMG')} dst
        USING (SELECT :1 AS B_MSISDN,:2 AS START_DATE,:3 AS START_HOUR,
                      :4 AS EVENT_TYPE,:5 AS SERVICE_TYPE FROM DUAL) src
        ON (dst.B_MSISDN=src.B_MSISDN AND dst.START_DATE=src.START_DATE
            AND dst.START_HOUR=src.START_HOUR
            AND NVL(dst.EVENT_TYPE,'__')=NVL(src.EVENT_TYPE,'__')
            AND NVL(dst.SERVICE_TYPE,'__')=NVL(src.SERVICE_TYPE,'__'))
        WHEN MATCHED THEN UPDATE SET
            dst.CDR_COUNT=dst.CDR_COUNT+:6, dst.etl_chargement=:7
        WHEN NOT MATCHED THEN INSERT
            (fichier_source,etl_chargement,B_MSISDN,START_DATE,START_HOUR,
             CALL_TYPE,EVENT_TYPE,EVENT_STATUS,SUBSCRIBER_TYPE,SERVICE_TYPE,CDR_COUNT)
        VALUES (:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18)
    """
    rows = []
    for r in agg.itertuples(index=False):
        bm  = to_none(getattr(r,"B_MSISDN",None))
        sd  = getattr(r,"START_DATE",None)
        sh  = getattr(r,"START_HOUR",None)
        shi = int(sh) if sh is not None else None
        et  = to_none(getattr(r,"EVENT_TYPE",None))
        st  = to_none(getattr(r,"SERVICE_TYPE",None))
        ct  = to_none(getattr(r,"CALL_TYPE",None))
        es  = to_none(getattr(r,"EVENT_STATUS",None))
        sub = to_none(getattr(r,"SUBSCRIBER_TYPE",None))
        cnt = int(r.CDR_COUNT)
        rows.append((bm,sd,shi,et,st, cnt,now, fname,now,bm,sd,shi,ct,et,es,sub,st,cnt))
    cur = conn.cursor()
    n = batch_execute(cur, conn, sql, rows)
    cur.close()
    log.info(f"  AGG_MMG : {n:,} lignes MERGE")
    return n


def aggregate_and_insert_occ(df, fname, conn):
    """Agrège les sessions OCC par B_MSISDN + date + heure + keyword → MERGE AGG_OCC."""
    now = datetime.now()
    if df.empty or "B_MSISDN" not in df.columns: return 0
    df = df.copy()

    charge_col = next((c for c in ["CHARGE_AMOUNT","CHARGE_AMOUNT_ORIG"] if c in df.columns), None)
    df["_charge"] = pd.to_numeric(df[charge_col], errors="coerce").fillna(0) if charge_col else 0.0

    kw_col = next((c for c in ["KEYWORD","SERVICE_TYPE"] if c in df.columns), None)
    df["_keyword"] = df[kw_col].apply(to_none) if kw_col else None

    ev_col = next((c for c in ["EVENT_TYPE_ORIG","EVENT_TYPE"] if c in df.columns), None)
    df["_event"] = df[ev_col].apply(to_none) if ev_col else None

    gk = [c for c in ["B_MSISDN","START_DATE","START_HOUR","_event",
                       "CALL_TYPE","SUBSCRIBER_TYPE","_keyword"] if c in df.columns]
    agg = (df.groupby(gk, dropna=False)
             .agg(CDR_COUNT=("B_MSISDN","count"), CHARGE_AMOUNT=("_charge","sum"))
             .reset_index())

    sql = f"""
        MERGE INTO {tbl('AGG_OCC')} dst
        USING (SELECT :1 AS B_MSISDN,:2 AS START_DATE,:3 AS START_HOUR,
                      :4 AS EVENT_TYPE,:5 AS KEYWORD FROM DUAL) src
        ON (dst.B_MSISDN=src.B_MSISDN AND dst.START_DATE=src.START_DATE
            AND dst.START_HOUR=src.START_HOUR
            AND NVL(dst.EVENT_TYPE,'__')=NVL(src.EVENT_TYPE,'__')
            AND NVL(dst.KEYWORD,'__')=NVL(src.KEYWORD,'__'))
        WHEN MATCHED THEN UPDATE SET
            dst.CDR_COUNT=dst.CDR_COUNT+:6,
            dst.CHARGE_AMOUNT=dst.CHARGE_AMOUNT+:7,
            dst.etl_chargement=:8
        WHEN NOT MATCHED THEN INSERT
            (fichier_source,etl_chargement,B_MSISDN,START_DATE,START_HOUR,
             CALL_TYPE,EVENT_TYPE,SUBSCRIBER_TYPE,KEYWORD,CDR_COUNT,CHARGE_AMOUNT)
        VALUES (:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19)
    """
    rows = []
    for r in agg.itertuples(index=False):
        bm  = to_none(getattr(r,"B_MSISDN",None))
        sd  = getattr(r,"START_DATE",None)
        sh  = getattr(r,"START_HOUR",None)
        shi = int(sh) if sh is not None else None
        et  = to_none(getattr(r,"_event",None))
        kw  = to_none(getattr(r,"_keyword",None))
        ct  = to_none(getattr(r,"CALL_TYPE",None))
        sub = to_none(getattr(r,"SUBSCRIBER_TYPE",None))
        cnt = int(r.CDR_COUNT)
        ch  = round(float(r.CHARGE_AMOUNT), 5)
        rows.append((bm,sd,shi,et,kw, cnt,ch,now, fname,now,bm,sd,shi,ct,et,sub,kw,cnt,ch))
    cur = conn.cursor()
    n = batch_execute(cur, conn, sql, rows)
    cur.close()
    log.info(f"  AGG_OCC : {n:,} lignes MERGE")
    return n
