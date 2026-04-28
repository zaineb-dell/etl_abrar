"""
run_dw.py — Alimente le Data Warehouse.

Flux :
  REF_SERVICE     -> DIM_SERVICE
  REF_FOURNISSEUR -> DIM_FOURNISSEUR
  AGG_MMG         -> DIM_TEMPS + DIM_ABONNE + FACT_REVENUS.sms_count
  AGG_OCC         -> DIM_TEMPS + DIM_ABONNE + FACT_REVENUS.session_count + total_revenue

Usage : py run_dw.py
"""

from datetime import datetime
import pandas as pd

from config import log
from db import get_conn, tbl
from warehouse import (
    upsert_dim_temps,
    upsert_dim_abonne,
    upsert_dim_service_from_ref,
    upsert_dim_fournisseur_from_ref,
    load_fact_revenus_from_agg,
)


def run_dw():
    log.info("=" * 65)
    log.info("DW CHARGEMENT")
    log.info("=" * 65)

    conn = get_conn()
    cur  = conn.cursor()

    # ── Etape 1 : DIM_SERVICE depuis REF_SERVICE ────────────────
    log.info("Etape 1 : DIM_SERVICE <- REF_SERVICE")
    svc_map = upsert_dim_service_from_ref(conn)

    # ── Etape 2 : DIM_FOURNISSEUR depuis REF_FOURNISSEUR ────────
    log.info("Etape 2 : DIM_FOURNISSEUR <- REF_FOURNISSEUR")
    four_map = upsert_dim_fournisseur_from_ref(conn)

    # ── Etape 3 : MMG ────────────────────────────────────────────
    log.info("Etape 3 : AGG_MMG -> DIM_ABONNE + DIM_TEMPS + FACT")
    cur.execute(f"SELECT DISTINCT NVL(fichier_source,'AGG_MMG') FROM {tbl('AGG_MMG')}")
    fichiers_mmg = [r[0] for r in cur.fetchall()]
    log.info(f"  {len(fichiers_mmg)} fichier(s) MMG")

    for fname in fichiers_mmg:
        log.info(f"  -> [{fname}]")
        cur.execute(f"""
            SELECT B_MSISDN, START_DATE, START_HOUR, SUBSCRIBER_TYPE
            FROM {tbl('AGG_MMG')} WHERE NVL(fichier_source,'AGG_MMG') = :1
        """, [fname])
        rows = cur.fetchall()
        if not rows:
            continue
        df = pd.DataFrame(rows, columns=["A_MSISDN","START_DATE","START_HOUR","SUBSCRIBER_TYPE"])

        def make_dt(r):
            try:
                sd = r["START_DATE"]
                if sd:
                    return datetime(sd.year, sd.month, sd.day, int(r["START_HOUR"] or 0), 0, 0)
            except Exception:
                pass
            return None

        df["_dt"] = df.apply(make_dt, axis=1)
        dt_map    = upsert_dim_temps(df["_dt"].tolist(), conn)
        ab_map    = upsert_dim_abonne(df, conn)
        load_fact_revenus_from_agg("MMG", fname, dt_map, ab_map, svc_map, four_map, conn)

    # ── Etape 4 : OCC ────────────────────────────────────────────
    log.info("Etape 4 : AGG_OCC -> DIM_ABONNE + DIM_TEMPS + FACT")
    cur.execute(f"SELECT DISTINCT NVL(fichier_source,'AGG_OCC') FROM {tbl('AGG_OCC')}")
    fichiers_occ = [r[0] for r in cur.fetchall()]
    log.info(f"  {len(fichiers_occ)} fichier(s) OCC")

    for fname in fichiers_occ:
        log.info(f"  -> [{fname}]")
        cur.execute(f"""
            SELECT B_MSISDN, START_DATE, START_HOUR, SUBSCRIBER_TYPE
            FROM {tbl('AGG_OCC')} WHERE NVL(fichier_source,'AGG_OCC') = :1
        """, [fname])
        rows = cur.fetchall()
        if not rows:
            continue
        df = pd.DataFrame(rows, columns=["A_MSISDN","START_DATE","START_HOUR","SUBSCRIBER_TYPE"])

        def make_dt(r):
            try:
                sd = r["START_DATE"]
                if sd:
                    return datetime(sd.year, sd.month, sd.day, int(r["START_HOUR"] or 0), 0, 0)
            except Exception:
                pass
            return None

        df["_dt"] = df.apply(make_dt, axis=1)
        dt_map    = upsert_dim_temps(df["_dt"].tolist(), conn)
        ab_map    = upsert_dim_abonne(df, conn)
        load_fact_revenus_from_agg("OCC", fname, dt_map, ab_map, svc_map, four_map, conn)

    # ── Stats finales ─────────────────────────────────────────────
    log.info("=" * 65)
    log.info("STATS FINALES")
    for t in ["FACT_REVENUS","DIM_ABONNE","DIM_TEMPS","DIM_SERVICE","DIM_FOURNISSEUR"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {tbl(t)}")
            log.info(f"  {tbl(t):45s} : {cur.fetchone()[0]:>10,}")
        except Exception:
            pass

    cur.close()
    conn.close()
    log.info("=" * 65)
    log.info("DW alimente avec succes")


if __name__ == "__main__":
    run_dw()
