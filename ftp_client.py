"""
ftp_client.py — Téléchargement FTP / SFTP des fichiers CDR.

Fonctionnalités :
  - Retry automatique avec backoff exponentiel
  - Registre des fichiers déjà téléchargés (évite les doublons)
  - Validation taille / extension avant téléchargement
  - Support FTP (ftplib) et SFTP (paramiko)
"""

import ftplib
import time
from pathlib import Path

from config import (
    FTP_ENABLED, FTP_PROTOCOL, FTP_HOST, FTP_PORT,
    FTP_USER, FTP_PASSWORD, FTP_KEY_PATH, FTP_REMOTE_DIR,
    FTP_EXTENSIONS, FTP_MIN_SIZE, FTP_MAX_RETRY, FTP_RETRY_WAIT,
    FTP_REGISTRY, log,
)

try:
    import paramiko
    _PARAMIKO_OK = True
except ImportError:
    _PARAMIKO_OK = False


# ── Registre ───────────────────────────────────────────────────

def _load_registry() -> set:
    if FTP_REGISTRY.exists():
        return set(FTP_REGISTRY.read_text(encoding="utf-8").splitlines())
    return set()


def _register(filename: str) -> None:
    FTP_REGISTRY.parent.mkdir(parents=True, exist_ok=True)
    with open(FTP_REGISTRY, "a", encoding="utf-8") as f:
        f.write(filename + "\n")


def _validate_remote(filename: str, size: int, registry: set) -> bool:
    if Path(filename).suffix.lower() not in FTP_EXTENSIONS:
        log.debug(f"[FTP] Ignoré (extension) : {filename}")
        return False
    if size < FTP_MIN_SIZE:
        log.warning(f"[FTP] Trop petit ({size} o), ignoré : {filename}")
        return False
    if filename in registry:
        log.info(f"[FTP] Déjà téléchargé, ignoré : {filename}")
        return False
    return True


# ── Retry ──────────────────────────────────────────────────────

def _with_retry(fn, *args, label="op", **kwargs):
    last = None
    for attempt in range(1, FTP_MAX_RETRY + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last = exc
            wait = FTP_RETRY_WAIT * (2 ** (attempt - 1))
            log.warning(
                f"[FTP] {label} — tentative {attempt}/{FTP_MAX_RETRY} "
                f"échouée ({exc}). Attente {wait}s…"
            )
            time.sleep(wait)
    raise last


# ── FTP ────────────────────────────────────────────────────────

def ftp_download(input_dir: Path) -> list[Path]:
    """Télécharge les nouveaux fichiers depuis un serveur FTP."""
    input_dir.mkdir(parents=True, exist_ok=True)
    registry = _load_registry()
    downloaded = []

    try:
        def _connect():
            f = ftplib.FTP()
            f.connect(FTP_HOST, FTP_PORT, timeout=30)
            f.login(FTP_USER, FTP_PASSWORD)
            f.cwd(FTP_REMOTE_DIR)
            return f

        log.info(f"[FTP] Connexion → {FTP_HOST}:{FTP_PORT}{FTP_REMOTE_DIR}")
        ftp = _with_retry(_connect, label="connexion FTP")

        entries = []
        ftp.retrlines("LIST", entries.append)

        for entry in entries:
            parts = entry.split()
            if len(parts) < 9:
                continue
            try:
                size = int(parts[4])
            except ValueError:
                continue
            filename = " ".join(parts[8:])
            if not _validate_remote(filename, size, registry):
                continue
            local_path = input_dir / filename
            log.info(f"[FTP] ↓ {filename} ({size:,} o)")

            def _download(fn=filename, lp=local_path):
                with open(lp, "wb") as f:
                    ftp.retrbinary(f"RETR {fn}", f.write)

            _with_retry(_download, label=f"téléchargement {filename}")
            _register(filename)
            registry.add(filename)
            downloaded.append(local_path)
            log.info(f"[FTP] ✓ {filename}")

        ftp.quit()

    except Exception as e:
        log.error(f"[FTP] Erreur : {e} — pipeline continue avec les fichiers locaux.")

    log.info(f"[FTP] {len(downloaded)} fichier(s) téléchargé(s)")
    return downloaded


# ── SFTP ───────────────────────────────────────────────────────

def sftp_download(input_dir: Path) -> list[Path]:
    """Télécharge les nouveaux fichiers depuis un serveur SFTP (paramiko)."""
    if not _PARAMIKO_OK:
        log.error("[SFTP] paramiko non installé — pip install paramiko")
        return []

    input_dir.mkdir(parents=True, exist_ok=True)
    registry = _load_registry()
    downloaded = []

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        kw = dict(hostname=FTP_HOST, port=FTP_PORT, username=FTP_USER, timeout=30)
        if FTP_KEY_PATH:
            kw["key_filename"] = FTP_KEY_PATH
        else:
            kw["password"] = FTP_PASSWORD

        log.info(f"[SFTP] Connexion → {FTP_HOST}:{FTP_PORT}{FTP_REMOTE_DIR}")
        _with_retry(ssh.connect, **kw, label="connexion SFTP")
        sftp = ssh.open_sftp()
        sftp.chdir(FTP_REMOTE_DIR)

        for attr in sftp.listdir_attr():
            if not _validate_remote(attr.filename, attr.st_size or 0, registry):
                continue
            local_path = input_dir / attr.filename
            log.info(f"[SFTP] ↓ {attr.filename}")
            _with_retry(sftp.get, attr.filename, str(local_path),
                        label=f"téléchargement {attr.filename}")
            _register(attr.filename)
            registry.add(attr.filename)
            downloaded.append(local_path)
            log.info(f"[SFTP] ✓ {attr.filename}")

        sftp.close()
        ssh.close()

    except Exception as e:
        log.error(f"[SFTP] Erreur : {e} — pipeline continue avec les fichiers locaux.")

    log.info(f"[SFTP] {len(downloaded)} fichier(s) téléchargé(s)")
    return downloaded


# ── Point d'entrée ─────────────────────────────────────────────

def pull_remote_files(input_dir: Path) -> list[Path]:
    """Choisit FTP ou SFTP selon FTP_PROTOCOL. Ne fait rien si désactivé."""
    if not FTP_ENABLED:
        log.info("[REMOTE] FTP_ENABLED=false — téléchargement désactivé.")
        return []
    if not FTP_HOST:
        log.warning("[REMOTE] FTP_HOST non défini — téléchargement ignoré.")
        return []
    if FTP_PROTOCOL == "sftp":
        return sftp_download(input_dir)
    return ftp_download(input_dir)
