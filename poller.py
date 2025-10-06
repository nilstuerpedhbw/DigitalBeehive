import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import pandas as pd

from constants import WETTERSTATION_AUTHT_GROUP, FUTTERKAMMER_AUTH_GROUP, BRUTKAMMER_AUTH_GROUP
from client import Client
from db.beehiveDbClient import BeehiveDbClient

# Lade Umgebungsvariablen
load_dotenv()

# Konfiguration
POLL_INTERVAL_SECONDS = 5 * 60  # 5 Minuten
LOOKBACK_MINUTES = 5  # Standard: letzte 5 Minuten
MAX_LOOKBACK_MINUTES = 60  # Maximal 1 Stunde zurückschauen

# AuthGroups für die 3 Bienenstöcke
AUTH_GROUPS = [
    ("Wetterstation", WETTERSTATION_AUTHT_GROUP),
    ("Futterkammer", FUTTERKAMMER_AUTH_GROUP),
    ("Brutkammer", BRUTKAMMER_AUTH_GROUP)
]

# Logging Setup
def setup_logger() -> logging.Logger:
    """Richtet Logger für Poller ein"""
    logger = logging.getLogger("beehive_poller")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    # Format
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )
    
    # Console Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    
    # Optional: File Handler (tägliche Log-Datei)
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    today = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d")
    fh = logging.FileHandler(log_dir / f"poller_{today}.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    
    return logger

logger = setup_logger()

class BeehivePoller:
    """Hauptklasse für 5-Minuten Polling der Bienenstock-Sensordaten"""
    
    def __init__(self):
        self.client = Client()
        self.db_client = None
        self.consecutive_errors = 0  # Zählt aufeinanderfolgende Fehler
        
        try:
            self.db_client = BeehiveDbClient(collection="digitalBeehive", isTimeSeries=True)
        except Exception as e:
            logger.error(f"MongoDB Initialisierung fehlgeschlagen: {e}")
            logger.error("Poller kann nicht starten ohne DB-Verbindung!")
            sys.exit(1)
    
    def calculate_lookback_minutes(self) -> int:
        """
        Berechnet wie viele Minuten zurückgeschaut werden sollen.
        Bei Fehlern: mehr Minuten, um Lücken zu füllen.
        """
        if self.consecutive_errors == 0:
            return LOOKBACK_MINUTES
        
        # Pro Fehler 5 Minuten mehr zurückschauen
        lookback = LOOKBACK_MINUTES + (self.consecutive_errors * POLL_INTERVAL_SECONDS // 60)
        return min(lookback, MAX_LOOKBACK_MINUTES)
    
    def get_time_range(self, lookback_minutes: int) -> tuple[str, str, str, str]:
        """
        Berechnet Start/End Zeitpunkte für API-Abfrage.
        
        Returns:
            (startDate, startTime, endDate, endTime) im Format der API
        """
        now = datetime.now(ZoneInfo("Europe/Berlin"))
        start = now - timedelta(minutes=lookback_minutes)
        
        return (
            start.strftime("%d.%m.%Y"),
            start.strftime("%H:%M"),
            now.strftime("%d.%m.%Y"),
            now.strftime("%H:%M")
        )
    
    def fetch_and_store_group(self, name: str, auth_group: str, lookback_minutes: int) -> bool:
        """
        Holt Daten für eine AuthGroup und speichert in MongoDB.
        
        Returns:
            True bei Erfolg, False bei Fehler
        """
        try:
            logger.info(f"Starte Datenabfrage: {name} (lookback={lookback_minutes}min)")
            
            start_date, start_time, end_date, end_time = self.get_time_range(lookback_minutes)
            
            # Hole alle Entity IDs
            entity_ids = self.client.get_all_entity_ids(auth_group)
            logger.info(f"{name}: {len(entity_ids)} Sensoren gefunden")
            
            all_rows = []
            
            # Für jede Entity Time-Series abrufen
            for entity_id in entity_ids:
                try:
                    data = self.client.get_time_series(
                        entityId=entity_id,
                        authGroup=auth_group,
                        startDate=start_date,
                        startTime=start_time,
                        endDate=end_date,
                        endTime=end_time
                    )
                    
                    # Normalisiere Daten
                    if isinstance(data, dict) and any(isinstance(v, dict) for v in data.values()):
                        for _, measurements in data.items():
                            all_rows.extend(
                                self.client._normalize_timeseries_payload(entity_id, measurements)
                            )
                    else:
                        all_rows.extend(
                            self.client._normalize_timeseries_payload(entity_id, data)
                        )
                
                except Exception as e:
                    logger.error(f"Fehler bei Entity {entity_id}: {e}")
                    all_rows.append({
                        "entityId": entity_id,
                        "key": None,
                        "ts": None,
                        "value": f"error: {str(e)}"
                    })
            
            # Erstelle DataFrame und konvertiere Zeitstempel
            df = pd.DataFrame(all_rows)
            df = self.client._to_berlin_datetime(df)
            
            logger.info(f"{name}: {len(df)} Datenpunkte abgerufen")
            
            # Speichere in MongoDB
            if not df.empty:
                result = self.db_client.insert_many(df)
                logger.info(
                    f"{name}: MongoDB Insert - {result['inserted']} neu, "
                    f"{result['duplicates']} Duplikate, {result['errors']} Fehler"
                )
            else:
                logger.warning(f"{name}: Keine Daten zum Speichern")
            
            return True
            
        except Exception as e:
            logger.error(f"Fehler bei {name}: {e}", exc_info=True)
            return False
    
    def poll_once(self):
        """Führt einen Polling-Zyklus aus"""
        lookback = self.calculate_lookback_minutes()
        
        logger.info(f"=== Polling-Zyklus gestartet (lookback={lookback}min) ===")
        
        success_count = 0
        
        for name, auth_group in AUTH_GROUPS:
            if self.fetch_and_store_group(name, auth_group, lookback):
                success_count += 1
        
        # Fehler-Counter anpassen
        if success_count == len(AUTH_GROUPS):
            if self.consecutive_errors > 0:
                logger.info(f"Alle Gruppen erfolgreich nach {self.consecutive_errors} Fehlern")
            self.consecutive_errors = 0
        else:
            self.consecutive_errors += 1
            logger.warning(
                f"Nur {success_count}/{len(AUTH_GROUPS)} Gruppen erfolgreich. "
                f"Consecutive Errors: {self.consecutive_errors}"
            )
        
        logger.info("=== Polling-Zyklus beendet ===\n")
    
    def run(self):
        """Startet den endlosen Polling-Loop"""
        logger.info("Beehive Poller gestartet")
        logger.info(f"Polling Intervall: {POLL_INTERVAL_SECONDS}s ({POLL_INTERVAL_SECONDS//60} Minuten)")
        logger.info(f"Überwachte Bienenstöcke: {len(AUTH_GROUPS)}")
        
        try:
            while True:
                try:
                    self.poll_once()
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    logger.error(f"Unerwarteter Fehler im Polling-Loop: {e}", exc_info=True)
                    self.consecutive_errors += 1
                
                # Warte bis zum nächsten Intervall
                logger.info(f"Warte {POLL_INTERVAL_SECONDS}s bis zum nächsten Poll...\n")
                time.sleep(POLL_INTERVAL_SECONDS)
        
        except KeyboardInterrupt:
            logger.info("\nPoller durch Benutzer gestoppt (Ctrl+C)")
        finally:
            logger.info("Beehive Poller beendet")

def main():
    poller = BeehivePoller()
    poller.run()

if __name__ == "__main__":
    main()