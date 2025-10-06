from __future__ import annotations

import os
import logging
import pandas as pd

from pymongo import MongoClient

from util.timeParser import TimeParser

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

class BeehiveDbClient:
    def __init__(self, collection: str = "digitalBeehive", isTimeSeries: bool = True) -> BeehiveDbClient:
        self.isTimeSeries = isTimeSeries

        mongo_client = MongoClient(os.getenv("MONGODB_URI"))
        self.logger = logging.getLogger("DbMongoCLient")
        try:
            self.collection = mongo_client["default"][collection]
            self.logger.log(logging.INFO, "Collection loaded succesfully")
        except Exception as e:
            self.logger.log(logging.ERROR, "Collection {collection} not found. ")

    def insert_many(self, df: pd.DataFrame) -> None:
        if self.isTimeSeries == True:
            tp = TimeParser()
            docs = tp.inject_bson_datetime(df, replace_ts=True).to_dict("records")
        else:
           docs = df.to_dict("records") 
        self.collection.insert_many(docs)

    def insert_one(self, entry: str) -> None:
        self.collection.insert_one(entry)

    # --------- UPDATE-FUNKTIONEN (minimal hinzugefügt) ---------

    def update_add_field_all(self, field: str, value):
        """Fügt allen Dokumenten ein (neues) Feld hinzu bzw. überschreibt es."""
        return self.collection.update_many({}, {"$set": {field: value}})

    def update_add_field_if_missing(self, field: str, value):
        """Fügt das Feld nur hinzu, wenn es nicht existiert."""
        return self.collection.update_many({field: {"$exists": False}}, {"$set": {field: value}})

    def update_one_set(self, query: dict, set_fields: dict):
        """Setzt Felder für ein einzelnes (erstes) Match."""
        return self.collection.update_one(query, {"$set": set_fields})

    def update_many_set(self, query: dict, set_fields: dict):
        """Setzt Felder für alle Dokumente, die dem Filter entsprechen."""
        return self.collection.update_many(query, {"$set": set_fields})

    def update_many_pipeline(self, query: dict, pipeline: list):
        """
        Update via Pipeline (MongoDB >= 4.2), z.B. berechnete Felder.
        Beispiel-Pipeline: [{"$set": {"tempC_avg": {"$avg": ["$TempC1","$TempC2","$TempC3"]}}}]
        """
        return self.collection.update_many(query, pipeline)

    def unset_fields(self, fields: list):
        """Entfernt Felder aus allen Dokumenten."""
        return self.collection.update_many({}, {"$unset": {f: "" for f in fields}})

    def rename_field(self, old: str, new: str):
        """Benennt ein Feld in allen Dokumenten um (falls vorhanden)."""
        return self.collection.update_many({old: {"$exists": True}}, {"$rename": {old: new}})
