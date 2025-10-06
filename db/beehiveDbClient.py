from __future__ import annotations

import os
import logging
from typing import Dict

import pandas as pd
from pymongo import MongoClient, errors

from util.timeParser import TimeParser

logger = logging.getLogger("beehive_poller")

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


class BeehiveDbClient:
    """MongoDB Client für Bienenstock-Sensordaten"""
    
    def __init__(self, collection: str = "digitalBeehive", isTimeSeries: bool = True):
        """
        Args:
            collection: Name der MongoDB Collection (default: "digitalBeehive")
            isTimeSeries: Ob TimeParser für Zeitstempel-Konvertierung genutzt werden soll
        """
        self.isTimeSeries = isTimeSeries
        
        # MongoDB Connection
        mongo_uri = os.getenv("MONGO_URI")
        if not mongo_uri:
            raise ValueError("MONGO_URI Umgebungsvariable nicht gesetzt!")
        
        try:
            mongo_client = MongoClient(mongo_uri)
            self.logger = logging.getLogger("DbMongoClient")
            self.collection = mongo_client["default"][collection]
            
            # Erstelle Unique Index für Duplikats-Vermeidung
            self._create_indexes()
            
            self.logger.log(logging.INFO, f"Collection '{collection}' loaded successfully")
            logger.info(f"MongoDB Verbindung erfolgreich: default.{collection}")
            
        except Exception as e:
            self.logger.log(logging.ERROR, f"Collection '{collection}' not found. {e}")
            logger.error(f"MongoDB Verbindung fehlgeschlagen: {e}")
            raise
    
    def _create_indexes(self):
        """Erstellt Unique Index auf (entityId, key, ts) um Duplikate zu verhindern"""
        try:
            self.collection.create_index(
                [("entityId", 1), ("key", 1), ("ts", 1)],
                unique=True,
                name="unique_sensor_reading"
            )
            logger.debug("Unique Index erstellt/überprüft")
        except Exception as e:
            logger.warning(f"Index-Erstellung fehlgeschlagen (evtl. existiert bereits): {e}")
    
    def insert_many(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Fügt DataFrame in MongoDB ein. Duplikate werden übersprungen.
        
        Args:
            df: DataFrame mit Sensordaten
            
        Returns:
            Dict mit 'inserted', 'duplicates', 'errors'
        """
        if df.empty:
            logger.warning("Leerer DataFrame übergeben, nichts zu speichern")
            return {"inserted": 0, "duplicates": 0, "errors": 0}
        
        # TimeParser für Zeitstempel-Konvertierung (wenn aktiviert)
        if self.isTimeSeries:
            tp = TimeParser()
            docs = tp.inject_bson_datetime(df, replace_ts=True).to_dict("records")
        else:
            docs = df.to_dict("records")
        
        # Einfügen mit Duplikats-Behandlung
        inserted = 0
        duplicates = 0
        errors_count = 0
        
        for doc in docs:
            try:
                self.collection.insert_one(doc)
                inserted += 1
            except errors.DuplicateKeyError:
                duplicates += 1
                logger.debug(
                    f"Duplikat übersprungen: entityId={doc.get('entityId')}, "
                    f"key={doc.get('key')}, ts={doc.get('ts')}"
                )
            except Exception as e:
                errors_count += 1
                logger.error(f"Fehler beim Einfügen: {e}")
                logger.debug(f"Problematisches Dokument: {doc}")
        
        # Logging des Ergebnisses
        logger.info(
            f"MongoDB Insert: {inserted} eingefügt, "
            f"{duplicates} Duplikate übersprungen, {errors_count} Fehler"
        )
        
        return {
            "inserted": inserted,
            "duplicates": duplicates,
            "errors": errors_count
        }
    
    def insert_one(self, entry: dict) -> bool:
        """
        Fügt ein einzelnes Dokument ein.
        
        Args:
            entry: Dictionary mit Sensordaten
            
        Returns:
            True bei Erfolg, False bei Fehler/Duplikat
        """
        try:
            self.collection.insert_one(entry)
            logger.debug(f"Dokument eingefügt: {entry.get('entityId', 'unknown')}")
            return True
        except errors.DuplicateKeyError:
            logger.debug(f"Duplikat übersprungen: {entry.get('entityId', 'unknown')}")
            return False
        except Exception as e:
            logger.error(f"Fehler beim Einfügen: {e}")
            logger.debug(f"Problematisches Dokument: {entry}")
            return False