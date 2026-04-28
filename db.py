"""
db.py — Connexion Oracle et fonctions utilitaires base de données.
"""

from datetime import datetime, date

import cx_Oracle
import pandas as pd

from config import (
    ORACLE_INSTANT_CLIENT, ORACLE_CONFIG, ORACLE_SCHEMA,
    COMMIT_EVERY, NULL_STR, log,
)


def sep(char="=", n=65) -> str:
    return char * n


def tbl(name: str) -> str:
    """Préfixe un nom de table avec le schéma Oracle."""
    return f"{ORACLE_SCHEMA}.{name}" if ORACLE_SCHEMA else name


def get_conn() -> cx_Oracle.Connection:
    """Initialise Oracle Instant Client et retourne une connexion."""
    try:
        cx_Oracle.init_oracle_client(lib_dir=ORACLE_INSTANT_CLIENT)
    except Exception as e:
        log.debug(f"init_oracle_client : {e}")
    dsn = cx_Oracle.makedsn(
        ORACLE_CONFIG["host"],
        ORACLE_CONFIG["port"],
        service_name=ORACLE_CONFIG["service_name"],
    )
    try:
        conn = cx_Oracle.connect(
            user=ORACLE_CONFIG["user"],
            password=ORACLE_CONFIG["password"],
            dsn=dsn,
            encoding="UTF-8",
        )
        conn.autocommit = False
        log.info(f"✓ Connecté Oracle {conn.version}")
        return conn
    except cx_Oracle.DatabaseError as e:
        err, = e.args
        log.error(f"Erreur Oracle {err.code} : {err.message}")
        raise


def is_ora_error(e: cx_Oracle.DatabaseError, code: int) -> bool:
    try:
        return e.args[0].code == code
    except Exception:
        return False


def table_exists(cur, name: str) -> bool:
    parts = name.upper().split(".")
    schema = parts[0] if len(parts) == 2 else None
    cur.execute(
        "SELECT COUNT(*) FROM ALL_TABLES WHERE TABLE_NAME=:1 AND OWNER=NVL(:2,USER)",
        [parts[-1], schema],
    )
    return cur.fetchone()[0] > 0


def batch_execute(cur, conn, sql: str, rows: list) -> int:
    """Exécute executemany par chunks et commite à chaque tranche."""
    total = 0
    for i in range(0, len(rows), COMMIT_EVERY):
        chunk = rows[i:i + COMMIT_EVERY]
        cur.executemany(sql, chunk)
        conn.commit()
        total += len(chunk)
        log.info(f"    -> {total}/{len(rows)} lignes")
    return total


def to_none(val):
    """Convertit les valeurs nulles/sentinelles en None Python."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, str) and val.strip() in NULL_STR:
        return None
    return val


# ── Parsing de dates ───────────────────────────────────────────

_DATE_FORMATS = [
    ("%Y%m%d%H%M%S", 14), ("%Y%m%d%H%M", 12), ("%Y%m%d", 8),
    ("%Y-%m-%d %H:%M:%S", 19), ("%Y-%m-%d %H:%M", 16), ("%Y-%m-%d", 10),
    ("%d/%m/%Y %H:%M:%S", 19), ("%d/%m/%Y", 10),
]


def to_date(val) -> datetime | None:
    """Conversion robuste vers datetime — gère le timezone OCC (+0100)."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, date):
        return datetime(val.year, val.month, val.day)
    s = str(val).strip()
    # Strip timezone OCC : "20260201105525+0100" → "20260201105525"
    if "+" in s and len(s) > 14:
        s = s.split("+")[0]
    if not s or s in NULL_STR:
        return None
    for fmt, length in _DATE_FORMATS:
        try:
            return datetime.strptime(s[:length], fmt)
        except (ValueError, TypeError):
            continue
    log.debug(f"to_date non parsé : '{val}'")
    return None


def trunc_hour(dt: datetime | None) -> datetime | None:
    """
    Tronque un datetime à l'heure (minutes/secondes → 0).
    Garantit la cohérence entre les clés dt_map et la reconstruction
    depuis AGG_OCC/AGG_MMG (START_DATE + START_HOUR sans secondes).
    """
    if dt is None:
        return None
    return datetime(dt.year, dt.month, dt.day, dt.hour, 0, 0)


def etl_log(conn, source, fichier, niveau, n_in, n_out, statut, message=""):
    """Insère un enregistrement dans ETL_LOG."""
    try:
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO {tbl('ETL_LOG')} "
            f"(source,fichier,niveau,lignes_entree,lignes_sortie,statut,message) "
            f"VALUES (:1,:2,:3,:4,:5,:6,:7)",
            [source, fichier, niveau, n_in, n_out, statut, str(message)[:2000]],
        )
        conn.commit()
        cur.close()
    except Exception as e:
        log.warning(f"ETL_LOG : {e}")
