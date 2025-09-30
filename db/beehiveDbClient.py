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
        
