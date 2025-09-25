# file: Projects/daily_export.py
import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo  

from constants import AUTH_GROUP_1, AUTH_GROUP_2, AUTH_GROUP_3
from client import Client

def setup_paths() -> tuple[Path, Path, str]:
    """
    Ermittelt die Basis-Pfade relativ zu diesem Script und erstellt
    den Tagesordner unter data/YYYY-MM-DD/.
    """
    # Basis: .../Projects
    base_dir = Path(__file__).resolve().parent

    # Datum in Berliner Zeit (für "heute")
    today_str = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d")

    # Tagesordner: Projects/data/YYYY-MM-DD
    day_dir = base_dir / "data" / today_str
    day_dir.mkdir(parents=True, exist_ok=True)

    # Logdatei in genau diesem Tagesordner
    log_file = day_dir / f"export_{today_str}.log"

    return day_dir, log_file, today_str

def setup_logger(log_file: Path) -> logging.Logger:
    """
    Richtet Logging ein, alle Logs in die Tages-Logdatei.
    """
    logger = logging.getLogger("daily_export")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

    # File-Handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Optional: Console-Handler (hilfreich beim manuellen Start)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger

def export_group(c: Client, day_dir: Path, today_str: str, auth_group: str, filename_prefix: str, logger: logging.Logger):
    """
    Holt die heutigen Time-Series für eine AuthGroup und speichert CSV in den Tagesordner.
    Fehler werden geloggt; der Job läuft weiter.
    """
    try:
        logger.info(f"Starte Export: {filename_prefix} (authGroup={auth_group})")
        # Verwende die DF-Variante (falls deine Methode anders heißt, hier anpassen):
        df = c.get_today_time_series_for_all_entities(auth_group)

        csv_path = day_dir / f"{filename_prefix}_{today_str}.csv"
        df.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")

        logger.info(f"Export erfolgreich: {csv_path}")
        logger.info(f"Zeilen: {len(df)} | Spalten: {list(df.columns)}")
    except Exception as e:
        logger.error(f"Fehler beim Export {filename_prefix}: {e}", exc_info=True)

def main():
    day_dir, log_file, today_str = setup_paths()
    logger = setup_logger(log_file)

    logger.info("=== Daily Export Job gestartet ===")
    logger.info(f"Tagesordner: {day_dir}")

    c = Client()

    # Nacheinander exportieren – unabhängig per try/except
    export_group(c, day_dir, today_str, AUTH_GROUP_1, "auth_group_1", logger)
    export_group(c, day_dir, today_str, AUTH_GROUP_2, "auth_group_2", logger)
    export_group(c, day_dir, today_str, AUTH_GROUP_3, "auth_group_3", logger)

    logger.info("=== Daily Export Job beendet ===")

if __name__ == "__main__":
    main()
