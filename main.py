import os
import sys
import logging
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, errors

from client import Client
from util.mapping import entity_to_beehives, entity_id_to_sensor
from util.timeParser import TimeParser
from constants2 import (
    WETTERSTATION_AUTHT_GROUP,
    FUTTERKAMMER_AUTH_GROUP,
    BRUTKAMMER_AUTH_GROUP,
    NORMAL_VALUES
)

# UTF-8 Konsole (vermeidet UnicodeEncodeError)
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

load_dotenv()

# Logging konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("BeehiveMain")

class BeehiveDbClient:
    """MongoDB Client für Bienenstock-Sensordaten"""

    def __init__(self, collection: str = "digitalBeehive"):
        mongo_uri = os.getenv("MONGO_URI")
        if not mongo_uri:
            raise ValueError("MONGO_URI Umgebungsvariable nicht gesetzt!")

        mongo_client = MongoClient(mongo_uri)
        self.collection = mongo_client["default"][collection]

        # Unique Index auf (entityId, key, ts)
        try:
            self.collection.create_index(
                [("entityId", 1), ("key", 1), ("ts", 1)],
                unique=True,
                name="unique_sensor_reading"
            )
        except Exception as e:
            logger.warning(f"Index-Erstellung fehlgeschlagen: {e}")

    def insert_many(self, df: pd.DataFrame) -> Dict[str, int]:
        if df.empty:
            return {"inserted": 0, "duplicates": 0, "errors": 0}

        df_clean = df.copy()
        if "datetime_utc" in df_clean.columns:
            df_clean = df_clean[df_clean["datetime_utc"].notna()]
        elif "datetime" in df_clean.columns:
            df_clean = df_clean[df_clean["datetime"].notna()]
        if "ts" in df_clean.columns:
            df_clean = df_clean[df_clean["ts"].notna()]

        if df_clean.empty:
            return {"inserted": 0, "duplicates": 0, "errors": 0}

        tp = TimeParser()
        docs = tp.inject_bson_datetime(df_clean, replace_ts=True).to_dict("records")

        inserted = 0
        duplicates = 0
        errors_count = 0

        for doc in docs:
            try:
                self.collection.insert_one(doc)
                inserted += 1
            except errors.DuplicateKeyError:
                duplicates += 1
            except Exception as e:
                errors_count += 1
                logger.error(f"Fehler beim Einfügen: {e}")

        return {
            "inserted": inserted,
            "duplicates": duplicates,
            "errors": errors_count
        }


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df[df["key"].notna() & (df["key"] != "beehiveId")].copy()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    if "ts" in df.columns:
        df = df.dropna(subset=["ts"])

    tp = TimeParser()
    df = tp.inject_bson_datetime(df, replace_ts=False)

    if "datetime_utc" not in df.columns:
        if "datetime" in df.columns:
            df["datetime_utc"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        else:
            df["datetime_utc"] = pd.NaT

    df["datetime_local"] = df["datetime_utc"].dt.tz_convert("Europe/Berlin")

    subset_cols = [c for c in ["entityId", "key", "ts"] if c in df.columns]
    if subset_cols:
        df = df.drop_duplicates(subset=subset_cols)

    df["sensorName"] = df["entityId"].map(entity_id_to_sensor)
    df["beehiveIds"] = df["entityId"].map(entity_to_beehives)

    if "datetime_local" in df.columns:
        df = df.sort_values(by=["datetime_local", "entityId", "key"], ignore_index=True)

    cols = [
        "datetime_local", "entityId", "sensorName",
        "key", "value", "beehiveIds", "datetime_utc", "ts"
    ]
    df = df[[c for c in cols if c in df.columns]]

    return df

def get_season(month: int) -> str:
    if month in [12, 1, 2]:
        return "Winter"
    elif month in [3, 4, 5]:
        return "Frühling"
    elif month in [6, 7, 8]:
        return "Sommer"
    else:
        return "Herbst"

def check_anomalies(df: pd.DataFrame):
    if df.empty:
        return ["Keine Daten vorhanden"]

    messages = []

    for _, row in df.iterrows():
        sensor = row.get("sensorName") or row.get("entityId")
        value = row["value"]
        key = row["key"]
        dt = row.get("datetime_local", datetime.now())
        season = get_season(dt.month)

        normal_range = None
        for area in NORMAL_VALUES:
            if sensor in NORMAL_VALUES[area]:
                if key in NORMAL_VALUES[area][sensor]:
                    normal_range = NORMAL_VALUES[area][sensor][key].get(season)
                    break

        if not normal_range:
            continue

        min_val, max_val = normal_range
        delta = 0.1 * (max_val - min_val)
        orange_min = min_val - delta
        orange_max = max_val + delta

        if value < min_val or value > max_val:
            messages.append(f"🔴 ALARM: {sensor} {key} = {value} (Grenze {min_val}-{max_val})")
        elif value < min_val + delta or value > max_val - delta:
            messages.append(f"🟠 VORWARNUNG: {sensor} {key} = {value} (Grenze {min_val}-{max_val})")
        else:
            messages.append(f"✅ OK: {sensor} {key} = {value}")

    return messages


def cleanup_old_csv(log_folder: str, days: int = 7):
    now = time.time()
    cutoff = now - days * 86400  # 7 Tage in Sekunden

    for f in os.listdir(log_folder):
        if f.endswith(".csv"):
            path = os.path.join(log_folder, f)
            if os.path.getmtime(path) < cutoff:
                try:
                    os.remove(path)
                    print(f"🗑️ Gelöscht: {path}")
                except Exception as e:
                    print(f"⚠️ Fehler beim Löschen von {path}: {e}")


def fetch_and_clean(auth_group: str, group_name: str) -> pd.DataFrame:
    c = Client()
    now = datetime.now(ZoneInfo("Europe/Berlin"))
    start = now - timedelta(minutes=5)

    start_date = start.strftime("%d.%m.%Y")
    start_time = start.strftime("%H:%M")
    end_date = now.strftime("%d.%m.%Y")
    end_time = now.strftime("%H:%M")

    print(f"\n=== {group_name} ({auth_group}) ===")
    entity_ids = c.get_all_entity_ids(auth_group)
    print(f"Gefundene Entity-IDs: {entity_ids}")

    all_rows = []

    for eid in entity_ids:
        print(f"\n--- Entity: {eid} ---")
        try:
            raw = c.get_time_series(
                entityId=eid,
                authGroup=auth_group,
                startDate=start_date,
                startTime=start_time,
                endDate=end_date,
                endTime=end_time
            )

            if isinstance(raw, dict) and any(isinstance(v, dict) for v in raw.values()):
                for key, measurements in raw.items():
                    if key.lower() == "beehiveid":
                        continue
                    all_rows.extend(c._normalize_timeseries_payload(eid, measurements))
            else:
                all_rows.extend(c._normalize_timeseries_payload(eid, raw))

        except Exception as e:
            print(f"⚠️ Fehler beim Abrufen von Entity {eid}: {e}")

    df = pd.DataFrame(all_rows)
    df = c._to_berlin_datetime(df)
    df_clean = clean_dataframe(df)

    print(f"\nBereinigt: {len(df_clean)} gültige Werte ({len(df) - len(df_clean)} entfernt)")
    if not df_clean.empty:
        print(df_clean.head(10).to_string(index=False))

    return df_clean


def main():
    log_folder = "Logs"
    os.makedirs(log_folder, exist_ok=True)

    db_client = BeehiveDbClient(collection="digitalBeehive")
    all_results = []

    for name, auth_group in [
        ("Wetterstation", WETTERSTATION_AUTHT_GROUP),
        ("Futterkammer", FUTTERKAMMER_AUTH_GROUP),
        ("Brutkammer", BRUTKAMMER_AUTH_GROUP),
    ]:
        df = fetch_and_clean(auth_group, name)
        if not df.empty:
            all_results.append((name, df))

    total_rows = sum(len(df) for _, df in all_results)
    print(f"\n=== Zusammenfassung: {total_rows} bereinigte Werte insgesamt ===")

    if total_rows > 0:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        for name, df in all_results:
            filename = os.path.join(log_folder, f"cleaned_{name.lower()}_{timestamp}.csv")
            df.to_csv(filename, index=False, sep=";", encoding="utf-8-sig")
            print(f"💾 Gespeichert: {filename}")

            print(f"\n=== Alarmcheck für {name} ===")
            messages = check_anomalies(df)
            if messages:
                for msg in messages:
                    print(msg)
            else:
                print("✅ Alle Werte im Normalbereich")

            print(f"\n=== Speichern in MongoDB für {name} ===")
            result = db_client.insert_many(df)
            print(f"MongoDB Insert: {result['inserted']} eingefügt, "
                  f"{result['duplicates']} Duplikate, {result['errors']} Fehler")

    cleanup_old_csv(log_folder)


if __name__ == "__main__":
    while True:
        main()
        print("\n⏱️ Warten 5 Minuten bis zum nächsten Abruf...\n")
        time.sleep(300)
