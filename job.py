# file: Projects/daily_export.py
import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo  

from pandas import DataFrame

from constants import WETTERSTATION_AUTHT_GROUP, FUTTERKAMMER_AUTH_GROUP, BRUTKAMMER_AUTH_GROUP
from client import Client
from db.beehiveDbClient import BeehiveDbClient

created_data_frames = []

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
        df = c.get_today_time_series_for_all_entities(auth_group)

        created_data_frames.append(df)

        csv_path = day_dir / f"{filename_prefix}_{today_str}.csv"
        df.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")

        logger.info(f"Export erfolgreich: {csv_path}")
        logger.info(f"Zeilen: {len(df)} | Spalten: {list(df.columns)}")
    except Exception as e:
        logger.error(f"Fehler beim Export {filename_prefix}: {e}", exc_info=True)

def insert_into_database(data_frames: list[DataFrame], logger: logging.Logger):
    dbClient = BeehiveDbClient()
    logger.log(logging.INFO, "Starting Insertion into DB")
    try:
        for df in data_frames:
            dbClient.insert_many(df)
        logging.info("Insertion Completed")
    except Exception as e:
        logging.error("Logging Failed with Error: " + str(e))



def main():
    day_dir, log_file, today_str = setup_paths()
    logger = setup_logger(log_file)

    logger.info("=== Daily Export Job gestartet ===")
    logger.info(f"Tagesordner: {day_dir}")

    c = Client()

    # Nacheinander exportieren – unabhängig per try/except
    export_group(c, day_dir, today_str, WETTERSTATION_AUTHT_GROUP, "Wetterstation", logger)
    export_group(c, day_dir, today_str, FUTTERKAMMER_AUTH_GROUP, "Futterkammer", logger)
    export_group(c, day_dir, today_str, BRUTKAMMER_AUTH_GROUP, "Brutkammer", logger)
    logger.info("Alle Daten wurden als Csv Datei gespeichert.")

    insert_into_database(created_data_frames, logger=logger )

    logger.info("=== Daily Export Job beendet ===")

if __name__ == "__main__":
    main()
