"""
main.py — Pipeline ETL Télécom v4.2 (architecture modulaire).

Point d'entrée principal. Orchestre les 4 niveaux pour chaque fichier CDR.

Usage :
  py main.py                        # Lance le pipeline immédiatement
  py main.py --schedule             # Démarre le scheduler nightly (02:00)
  py main.py --schedule --now       # Scheduler + lancement immédiat
  py main.py --schedule --heure 03:30
"""

import argparse
import signal
import sys
from datetime import datetime
from pathlib import Path

from config import (
    INPUT_DIR, ARCHIVE_DIR, SCHEDULE_HEURE, SCHEDULE_MINUTE, log,
)
from db import get_conn, etl_log, sep, tbl
from ddl import init_all_tables
from extract import extract_local, detect_type, validate_schema
from ftp_client import pull_remote_files
from load import insert_raw, insert_detail, aggregate_and_insert_mmg, aggregate_and_insert_occ
from transform import transform_mmg, transform_occ
from warehouse import (
    upsert_dim_temps, upsert_dim_abonne,
    upsert_dim_service, upsert_dim_fournisseur,
    load_fact_revenus_from_agg,
)


def archive_file(path: Path) -> None:
    """Déplace le fichier traité vers le dossier archive."""
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    dest = ARCHIVE_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{path.name}"
    path.rename(dest)
    log.info(f"  Archivé : {dest.name}")


def process_file(fname: str, df_raw, conn) -> None:
    """
    Traite un fichier CDR à travers les 4 niveaux :
      Niveau 1 → RAW
      Niveau 2 → DETAIL (transformations)
      Niveau 3 → AGG (agrégation)
      Niveau 4 → DW (dimensions + FACT_REVENUS)
    """
    n_in = len(df_raw)
    try:
        dtype = detect_type(df_raw)
        log.info(f"\n{'─'*50}")
        log.info(f"  [{fname}] type={dtype}  lignes brutes={n_in:,}")
        validate_schema(df_raw, dtype)

        # ── Niveau 1 : RAW ──────────────────────────────────
        log.info(f"  → Niveau 1 : RAW_{dtype}")
        insert_raw(df_raw, dtype, fname, conn)
        etl_log(conn, dtype, fname, "RAW", n_in, n_in, "OK")

        # ── Niveau 2 : DETAIL ───────────────────────────────
        log.info(f"  → Niveau 2 : DETAIL_{dtype}")
        df = transform_mmg(df_raw, fname) if dtype == "MMG" else transform_occ(df_raw, fname)
        n_detail = insert_detail(df, dtype, fname, conn)
        etl_log(conn, dtype, fname, "DETAIL", n_in, n_detail, "OK")

        # ── Niveau 3 : AGG ──────────────────────────────────
        log.info(f"  → Niveau 3 : AGG_{dtype}")
        n_agg = (
            aggregate_and_insert_mmg(df, fname, conn) if dtype == "MMG"
            else aggregate_and_insert_occ(df, fname, conn)
        )
        etl_log(conn, dtype, fname, "AGG", n_detail, n_agg, "OK")

        # ── Niveau 4 : Data Warehouse ───────────────────────
        log.info(f"  → Niveau 4 : Data Warehouse")
        dt_map   = upsert_dim_temps(df["_dt"].tolist() if "_dt" in df.columns else [], conn)
        ab_map   = upsert_dim_abonne(df, conn)
        svc_map  = upsert_dim_service(df, dtype, conn)
        four_map = upsert_dim_fournisseur(df, conn)
        n_fact   = load_fact_revenus_from_agg(dtype, fname, dt_map, ab_map, svc_map, four_map, conn)
        etl_log(conn, dtype, fname, "DW", n_agg, n_fact, "OK")

        log.info(
            f"  [{fname}] ✓ TERMINÉ — "
            f"RAW:{n_in:,} | DETAIL:{n_detail:,} | AGG:{n_agg:,} | FACT:{n_fact:,}"
        )

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        log.error(f"  [{fname}] ERREUR : {e}", exc_info=True)
        etl_log(conn, "ERREUR", fname, "ERREUR", n_in, 0, "ERREUR", str(e))
        raise


def run_pipeline() -> None:
    """Lance le pipeline complet sur tous les fichiers de INPUT_DIR."""
    log.info(sep())
    log.info("ETL TÉLÉCOM v4.2 — RAW + DETAIL + AGG + DW  (architecture modulaire)")
    log.info(sep())

    conn = get_conn()
    init_all_tables(conn)
    pull_remote_files(INPUT_DIR)

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    fichiers = list(INPUT_DIR.glob("*.csv")) + list(INPUT_DIR.glob("*.xlsx"))

    if not fichiers:
        log.warning("Aucun fichier à traiter dans INPUT_DIR.")
        conn.close()
        return

    for path in fichiers:
        try:
            df_raw = extract_local(path)
            process_file(path.name, df_raw, conn)
            archive_file(path)
        except Exception:
            log.error(f"Fichier ignoré (erreur) : {path.name}")

    # ── Stats finales ────────────────────────────────────────
    cur = conn.cursor()
    tables = [
        "RAW_MMG", "RAW_OCC", "DETAIL_MMG", "DETAIL_OCC",
        "AGG_MMG", "AGG_OCC", "FACT_REVENUS",
        "DIM_ABONNE", "DIM_TEMPS", "DIM_SERVICE",
    ]
    log.info(sep())
    log.info("STATS FINALES")
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {tbl(t)}")
            log.info(f"  {tbl(t):45s} : {cur.fetchone()[0]:>10,}")
        except Exception:
            log.info(f"  {tbl(t):45s} : ?")
    cur.close()
    conn.close()
    log.info(sep())


def start_scheduler(heure: int, minute: int, run_now: bool) -> None:
    """Démarre le scheduler APScheduler pour un lancement nightly."""
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

    scheduler = BlockingScheduler(timezone="Africa/Tunis")
    scheduler.add_listener(
        lambda e: log.info("[SCHEDULER] Job terminé avec succès."),
        EVENT_JOB_EXECUTED,
    )
    scheduler.add_listener(
        lambda e: log.error(f"[SCHEDULER] Job échoué : {e.exception}"),
        EVENT_JOB_ERROR,
    )
    scheduler.add_job(
        func=run_pipeline,
        trigger=CronTrigger(hour=heure, minute=minute),
        id="etl_nightly",
        misfire_grace_time=3600,
        coalesce=True,
    )

    signal.signal(signal.SIGTERM, lambda s, f: (scheduler.shutdown(wait=False), sys.exit(0)))
    signal.signal(signal.SIGINT,  lambda s, f: (scheduler.shutdown(wait=False), sys.exit(0)))

    log.info(sep())
    log.info(f"SCHEDULER DÉMARRÉ — nightly {heure:02d}:{minute:02d} (Africa/Tunis)")
    log.info(sep())

    if run_now:
        log.info("[SCHEDULER] Lancement immédiat (--now)…")
        run_pipeline()

    scheduler.start()


# ── Point d'entrée ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Télécom v4.2 — architecture modulaire")
    parser.add_argument("--schedule", action="store_true", help="Démarrer le scheduler nightly")
    parser.add_argument("--now",      action="store_true", help="Lancer le pipeline immédiatement")
    parser.add_argument(
        "--heure", type=str,
        default=f"{SCHEDULE_HEURE:02d}:{SCHEDULE_MINUTE:02d}",
        help="Heure scheduler HH:MM (défaut: 02:00)",
    )
    args = parser.parse_args()

    if args.schedule:
        try:
            h, m = args.heure.split(":")
        except ValueError:
            print("Format --heure invalide. Utiliser HH:MM")
            sys.exit(1)
        start_scheduler(int(h), int(m), args.now)
    else:
        run_pipeline()
