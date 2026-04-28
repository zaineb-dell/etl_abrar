"""
warehouse.py — Niveau 4 : alimentation du Data Warehouse etoile.

Flux d'alimentation :
  REF_SERVICE     -> DIM_SERVICE     (keyword, nom, prix, fournisseur)
  REF_FOURNISSEUR -> DIM_FOURNISSEUR (provider_name, nationalite, adresse)
  AGG_MMG/OCC     -> DIM_ABONNE      (B_MSISDN, subscriber_type)
  AGG_MMG/OCC     -> DIM_TEMPS       (START_DATE + START_HOUR)
  AGG_MMG/OCC     -> FACT_REVENUS    (sms_count, session_count, total_revenue)
"""

from datetime import datetime
import pandas as pd
from config import MONTH_FR, DAY_FR, SVC_UNKNOWN, NULL_STR, log
from db import tbl, batch_execute, to_none, trunc_hour


# ── DIM_TEMPS ──────────────────────────────────────────────────

def upsert_dim_temps(datetimes: list, conn) -> dict:
    cur = conn.cursor()
    unique_dts = {trunc_hour(dt) for dt in datetimes if dt and isinstance(dt, datetime)}
    unique_dts.discard(None)
    if not unique_dts:
        cur.close()
        return {}
    rows = []
    for dt in unique_dts:
        wd = dt.weekday()
        rows.append({
            "date_id":    int(dt.strftime("%Y%m%d%H")),
            "full_date":  dt,
            "year":       dt.year,
            "month":      dt.month,
            "month_name": MONTH_FR.get(dt.month, ""),
            "day":        dt.day,
            "day_name":   DAY_FR.get(wd, ""),
            "hour":       dt.hour,
            "week":       int(dt.strftime("%W")),
            "quarter":    (dt.month - 1) // 3 + 1,
            "is_weekend": "O" if wd >= 5 else "N",
        })
    sql = f"""
        MERGE INTO {tbl('DIM_TEMPS')} dst
        USING (SELECT :date_id AS date_id FROM DUAL) src
        ON (dst.date_id = src.date_id)
        WHEN NOT MATCHED THEN INSERT
            (date_id,full_date,year,month,month_name,day,day_name,hour,week,quarter,is_weekend)
        VALUES
            (:date_id,:full_date,:year,:month,:month_name,:day,:day_name,:hour,:week,:quarter,:is_weekend)
    """
    batch_execute(cur, conn, sql, rows)
    mapping = {dt: int(dt.strftime("%Y%m%d%H")) for dt in unique_dts}
    cur.close()
    log.info(f"  DIM_TEMPS : {len(mapping):,} entrees")
    return mapping


# ── DIM_ABONNE ─────────────────────────────────────────────────

def upsert_dim_abonne(df: pd.DataFrame, conn) -> dict:
    cur = conn.cursor()
    now = datetime.now()
    msisdn_col = (
        "A_MSISDN" if "A_MSISDN" in df.columns
        else ("B_MSISDN" if "B_MSISDN" in df.columns
        else ("MSISDN"   if "MSISDN"   in df.columns else None))
    )
    if msisdn_col is None:
        cur.close()
        log.warning("  DIM_ABONNE : colonne MSISDN introuvable")
        return {}
    stype_col = df["SUBSCRIBER_TYPE"] if "SUBSCRIBER_TYPE" in df.columns else pd.Series([None]*len(df), index=df.index)
    sub = pd.DataFrame({
        "msisdn": df[msisdn_col].astype(str).str.strip(),
        "stype":  stype_col,
    })
    sub = sub[~sub["msisdn"].isin(NULL_STR) & sub["msisdn"].notna()]
    sub = sub.drop_duplicates(subset=["msisdn"])
    if sub.empty:
        cur.close()
        return {}
    rows = [
        {"msisdn": m, "stype": to_none(s), "now": now}
        for m, s in zip(sub["msisdn"], sub["stype"])
    ]
    sql = f"""
        MERGE INTO {tbl('DIM_ABONNE')} dst
        USING (SELECT :msisdn AS msisdn FROM DUAL) src
        ON (dst.msisdn = src.msisdn)
        WHEN MATCHED THEN UPDATE SET
            dst.last_seen       = :now,
            dst.subscriber_type = NVL(:stype, dst.subscriber_type)
        WHEN NOT MATCHED THEN INSERT
            (msisdn, subscriber_type, first_seen, last_seen,
             total_volume_mo, total_sms, total_revenue)
        VALUES (:msisdn, :stype, :now, :now, 0, 0, 0)
    """
    batch_execute(cur, conn, sql, rows)
    msisdns = [r["msisdn"] for r in rows]
    mapping = {}
    for i in range(0, len(msisdns), 900):
        chunk = msisdns[i:i+900]
        placeholders = ",".join(f":{j+1}" for j in range(len(chunk)))
        cur.execute(
            f"SELECT msisdn, abonne_id FROM {tbl('DIM_ABONNE')} WHERE msisdn IN ({placeholders})",
            chunk,
        )
        mapping.update({row[0]: row[1] for row in cur.fetchall()})
    cur.close()
    log.info(f"  DIM_ABONNE  : {len(mapping):,} abonnes")
    return mapping


# ── DIM_SERVICE — alimentee depuis REF_SERVICE ─────────────────

def upsert_dim_service_from_ref(conn) -> dict:
    """
    Alimente DIM_SERVICE depuis REF_SERVICE.
    Cle de jointure : keyword.
    Retourne {keyword -> service_id}.
    """
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {tbl('REF_SERVICE')}")
        count = cur.fetchone()[0]
        if count == 0:
            log.warning("  REF_SERVICE vide")
            cur.close()
            return {}
    except Exception:
        log.warning("  REF_SERVICE introuvable")
        cur.close()
        return {}

    cur.execute(f"""
        MERGE INTO {tbl('DIM_SERVICE')} dst
        USING (SELECT keyword, nom_service, nom_fournisseur,
                      numero_court, type_service, prix
               FROM {tbl('REF_SERVICE')}) src
        ON (dst.keyword = src.keyword)
        WHEN MATCHED THEN UPDATE SET
            dst.nom_service     = src.nom_service,
            dst.nom_fournisseur = src.nom_fournisseur,
            dst.numero_court    = src.numero_court,
            dst.type_service    = src.type_service,
            dst.prix            = src.prix
        WHEN NOT MATCHED THEN INSERT
            (keyword, nom_service, nom_fournisseur, numero_court, type_service, prix)
        VALUES
            (src.keyword, src.nom_service, src.nom_fournisseur,
             src.numero_court, src.type_service, src.prix)
    """)
    conn.commit()

    cur.execute(f"SELECT keyword, service_id FROM {tbl('DIM_SERVICE')}")
    mapping = {to_none(r[0]): r[1] for r in cur.fetchall() if r[0]}
    cur.close()
    log.info(f"  DIM_SERVICE : {len(mapping):,} services")
    return mapping


# ── DIM_FOURNISSEUR — alimentee depuis REF_FOURNISSEUR ─────────

def upsert_dim_fournisseur_from_ref(conn) -> dict:
    """
    Alimente DIM_FOURNISSEUR depuis REF_FOURNISSEUR.
    Retourne {provider_name -> fournisseur_id}.
    """
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {tbl('REF_FOURNISSEUR')}")
        count = cur.fetchone()[0]
        if count == 0:
            log.warning("  REF_FOURNISSEUR vide")
            cur.close()
            return {}
    except Exception:
        log.warning("  REF_FOURNISSEUR introuvable")
        cur.close()
        return {}

    cur.execute(f"""
        MERGE INTO {tbl('DIM_FOURNISSEUR')} dst
        USING (SELECT provider_name, nationalite, id_fiscale, adresse
               FROM {tbl('REF_FOURNISSEUR')}) src
        ON (dst.provider_name = src.provider_name)
        WHEN MATCHED THEN UPDATE SET
            dst.nationalite = src.nationalite,
            dst.id_fiscale  = src.id_fiscale,
            dst.adresse     = src.adresse
        WHEN NOT MATCHED THEN INSERT
            (provider_name, nationalite, id_fiscale, adresse)
        VALUES
            (src.provider_name, src.nationalite, src.id_fiscale, src.adresse)
    """)
    conn.commit()

    cur.execute(f"SELECT provider_name, fournisseur_id FROM {tbl('DIM_FOURNISSEUR')}")
    mapping = {to_none(r[0]): r[1] for r in cur.fetchall() if r[0]}
    cur.close()
    log.info(f"  DIM_FOURNISSEUR : {len(mapping):,} fournisseurs")
    return mapping


# ── Compatibilite avec main.py ─────────────────────────────────

def upsert_dim_service(df: pd.DataFrame, dtype: str, conn) -> dict:
    """Appelle upsert_dim_service_from_ref — la source est REF_SERVICE."""
    return upsert_dim_service_from_ref(conn)


def upsert_dim_fournisseur(df: pd.DataFrame, conn) -> dict:
    """Appelle upsert_dim_fournisseur_from_ref — la source est REF_FOURNISSEUR."""
    return upsert_dim_fournisseur_from_ref(conn)


# ── FACT_REVENUS ───────────────────────────────────────────────

def load_fact_revenus_from_agg(
    dtype: str, fname: str,
    dt_map: dict, ab_map: dict, svc_map: dict, four_map: dict,
    conn,
) -> int:
    """
    Alimente FACT_REVENUS depuis AGG_MMG ou AGG_OCC.
    svc_map  : {keyword -> service_id}      depuis DIM_SERVICE
    four_map : {provider_name -> fournisseur_id} depuis DIM_FOURNISSEUR
    """
    cur = conn.cursor()
    now = datetime.now()

    if dtype == "MMG":
        cur.execute(f"""
            SELECT B_MSISDN, START_DATE, START_HOUR, SERVICE_TYPE, CDR_COUNT
            FROM {tbl('AGG_MMG')} WHERE NVL(fichier_source,'AGG_MMG') = :1
        """, [fname])
        rows_db = cur.fetchall()

        fact_rows = []
        skip_dt = skip_ab = skip_svc = 0
        for r in rows_db:
            bmsisdn, sd, sh, stype, cnt = r[0], r[1], r[2], r[3], int(r[4] or 0)
            dt = datetime(sd.year, sd.month, sd.day, int(sh or 0), 0, 0) if sd else None
            date_id   = dt_map.get(dt)
            abonne_id = ab_map.get(str(bmsisdn))
            svc_id    = svc_map.get(to_none(stype))
            if date_id   is None: skip_dt  += 1; continue
            if abonne_id is None: skip_ab  += 1; continue
            if not svc_id:        skip_svc += 1; continue
            di, ai, si = int(date_id), int(abonne_id), int(svc_id)
            fact_rows.append((
                di, ai, si, 0,  0.0, 0.0, 0, 0,  cnt, 0, 0, 0, 0, 0.0, 0.0,  fname, now,
                di, ai, si, 0,  0.0, 0.0, 0, 0,  cnt, 0, 0, 0, 0, 0.0, 0.0,  fname, now,
            ))
        if skip_dt or skip_ab or skip_svc:
            log.warning(f"  [MMG FACT] Ignores — date:{skip_dt} abonne:{skip_ab} service:{skip_svc}")

    else:  # OCC
        cur.execute(f"""
            SELECT B_MSISDN, START_DATE, START_HOUR, KEYWORD, CDR_COUNT, CHARGE_AMOUNT
            FROM {tbl('AGG_OCC')} WHERE NVL(fichier_source,'AGG_OCC') = :1
        """, [fname])
        rows_db = cur.fetchall()

        fact_rows = []
        skip_dt = skip_ab = skip_svc = 0
        for r in rows_db:
            bmsisdn, sd, sh, keyword = r[0], r[1], r[2], r[3]
            cnt = int(r[4] or 0)
            try:
                rev = round(float(str(r[5] or 0).replace(',', '.')), 6)
            except Exception:
                rev = 0.0
            dt = datetime(sd.year, sd.month, sd.day, int(sh or 0), 0, 0) if sd else None
            date_id   = dt_map.get(dt)
            abonne_id = ab_map.get(str(bmsisdn))
            svc_id    = svc_map.get(to_none(keyword))
            if date_id   is None: skip_dt  += 1; continue
            if abonne_id is None: skip_ab  += 1; continue
            if not svc_id:        skip_svc += 1; continue
            di, ai, si = int(date_id), int(abonne_id), int(svc_id)
            fact_rows.append((
                di, ai, si, 0,  0.0, rev, cnt, 0,  0, 0, 0, 0, 0, 0.0, rev,  fname, now,
                di, ai, si, 0,  0.0, rev, cnt, 0,  0, 0, 0, 0, 0, 0.0, rev,  fname, now,
            ))
        if skip_dt or skip_ab or skip_svc:
            log.warning(f"  [OCC FACT] Ignores — date:{skip_dt} abonne:{skip_ab} service:{skip_svc}")

    sql = f"""
        MERGE INTO {tbl('FACT_REVENUS')} fr
        USING (SELECT :1 AS date_id, :2 AS abonne_id, :3 AS service_id FROM DUAL) src
        ON (fr.date_id=src.date_id AND fr.abonne_id=src.abonne_id AND fr.service_id=src.service_id)
        WHEN MATCHED THEN UPDATE SET
            fr.fournisseur_id  = :4,
            fr.data_volume_mo  = fr.data_volume_mo  + :5,
            fr.data_revenue    = fr.data_revenue    + :6,
            fr.session_count   = fr.session_count   + :7,
            fr.session_dur_sec = fr.session_dur_sec + :8,
            fr.sms_count       = fr.sms_count       + :9,
            fr.sms_success     = fr.sms_success     + :10,
            fr.sms_failed      = fr.sms_failed      + :11,
            fr.sms_mo          = fr.sms_mo          + :12,
            fr.sms_mt          = fr.sms_mt          + :13,
            fr.sms_revenue     = fr.sms_revenue     + :14,
            fr.total_revenue   = fr.total_revenue   + :15,
            fr.etl_source      = :16,
            fr.etl_chargement  = :17
        WHEN NOT MATCHED THEN INSERT
            (date_id,abonne_id,service_id,fournisseur_id,data_volume_mo,data_revenue,
             session_count,session_dur_sec,sms_count,sms_success,sms_failed,
             sms_mo,sms_mt,sms_revenue,total_revenue,etl_source,etl_chargement)
        VALUES (:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34)
    """
    n = batch_execute(cur, conn, sql, fact_rows)
    cur.close()
    log.info(f"  FACT_REVENUS [{dtype}] : {n:,} lignes MERGE")
    return n