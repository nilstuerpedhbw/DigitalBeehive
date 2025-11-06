import os
import sys
import time
import json
import smtplib
import logging
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from pymongo import MongoClient, errors
from dotenv import load_dotenv

from client import Client
from util.mapping import entity_to_beehives, entity_id_to_sensor
from util.timeParser import TimeParser
from constants2 import (
    WETTERSTATION_AUTHT_GROUP,
    FUTTERKAMMER_AUTH_GROUP,
    BRUTKAMMER_AUTH_GROUP,
    THRESHOLDS,
    SWING_THRESHOLD,
    BROOD_START_TEMP
)


try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# ============================================================
# Setup
# ============================================================
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("BeehiveMain")
   # speichert gesendete Alarme & Brutzeitstatus
DATA_FOLDER= "data"
STATE_FILE= "state.json"

# ============================================================
# Persistent States (wird automatisch gespeichert)
# ============================================================
_daily_sent = {}
_brood_status = {}

def load_state():
    """Lädt gespeicherte Zustände (falls vorhanden)."""
    
    os.makedirs(DATA_FOLDER, exist_ok=True)
    state_path = os.path.join(DATA_FOLDER, STATE_FILE)
    
    global _daily_sent, _brood_status
    if os.path.exists(state_path):
        try:
            with open(state_path , "r", encoding="utf-8") as f:
                state = json.load(f)
            _daily_sent = {datetime.strptime(k, "%Y-%m-%d").date(): set(tuple(x) for x in v) for k, v in state.get("daily_sent", {}).items()}
            _brood_status = {int(k): v for k, v in state.get("brood_status", {}).items()}
            logger.info(f"State aus {state_path} geladen")
        except Exception as e:
            logger.warning(f"Konnte {state_path} nicht laden: {e}")
    else:
        logger.info("Keine State-File gefunden")
        
def save_state():
    """Speichert aktuelle Zustände."""
    state_path = os.path.join(DATA_FOLDER, STATE_FILE)
    
    try:
        data = {
            "daily_sent": {str(k): [list(x) for x in v] for k, v in _daily_sent.items()},
            "brood_status": _brood_status
        }
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Fehler beim Speichern des State-Files: {e}")

# ============================================================
# Email Versand
# ============================================================
def send_email(subject: str, body: str):
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port   = int(os.getenv("SMTP_PORT", 587))
    smtp_user   = os.getenv("SMTP_USER")
    smtp_pass   = os.getenv("SMTP_PASS")
    alert_email = os.getenv("ALERT_EMAIL")

    if not (smtp_server and smtp_user and smtp_pass):
        logger.warning("E-Mail-Alarm deaktiviert – SMTP-Daten fehlen")
        return

    msg = MIMEText(body)
    msg["From"] = smtp_user
    msg["To"]   = alert_email
    msg["Subject"] = subject

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, [alert_email], msg.as_string())
            logger.info(f"📧 Alarm-Mail gesendet an {alert_email}")
    except Exception as e:
        logger.error(f"Fehler beim E-Mail Versand: {e}")

# ============================================================
# Datenaufbereitung & Analyse
# ============================================================
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df[df["key"].notna() & (df["key"] != "beehiveId")].copy()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value", "ts"])
    tp = TimeParser()
    df = tp.inject_bson_datetime(df, replace_ts=False)
    if "datetime_utc" not in df.columns:
        if "datetime" in df.columns:
            df["datetime_utc"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        else:
            df["datetime_utc"] = pd.NaT
    df["datetime_local"] = df["datetime_utc"].dt.tz_convert("Europe/Berlin")
    df["sensorName"] = df["entityId"].map(entity_id_to_sensor)
    df["beehiveIds"]  = df["entityId"].map(entity_to_beehives)
    return df

# ============================================================
# Anomalieerkennung
# ============================================================
def check_anomalies(df: pd.DataFrame, area: str):
    if df.empty:
        return

    global _daily_sent, _brood_status

    today = datetime.now().date()
    if today not in _daily_sent:
        _daily_sent[today] = set()

    for _, row in df.iterrows():
        sensor = row.get("sensorName", "")
        key    = row["key"]
        value  = row["value"]
        dt     = row["datetime_local"]

        # === 1. Starke Temperaturschwankungen ===
        prev_key = f"prev_{sensor}_{key}"
        prev_val = getattr(check_anomalies, prev_key, None)
        if key == "temperature" and prev_val is not None:
            diff = abs(value - prev_val)
            delta_min = (dt - getattr(check_anomalies, f"{prev_key}_time")).total_seconds() / 60.0
            if delta_min <= 10 and diff >= SWING_THRESHOLD:
                subject = f"[BEEHIVE] Große Temperaturschwankung bei {sensor}"
                body = f"Sensor {sensor}: Änderung {diff:.1f}°C in {delta_min:.1f} Minuten (aktuell {value}°C)."
                if ("swing", sensor, key) not in _daily_sent[today]:
                    send_email(subject, body)
                    _daily_sent[today].add(("swing", sensor, key))
                    print(f"🟠 Schwankung erkannt: {body}")

        setattr(check_anomalies, prev_key, value)
        setattr(check_anomalies, f"{prev_key}_time", dt)

        # === 2. Grenzwerte prüfen ===
        if key not in THRESHOLDS.get(area, {}):
            continue

        for zone in THRESHOLDS[area][key]:
            low, high = zone["range"]
            if low <= value < high:
                status = zone["status"]
                color  = zone["color"]
                type_id = None

                if color == "Rot":
                    type_id = ("alarm", sensor, key)
                elif color == "Orange":
                    type_id = ("warnung", sensor, key)

                if type_id and type_id not in _daily_sent[today]:
                    subject = f"[BEEHIVE] {status}: {area} {key} ({sensor})"
                    body    = f"{sensor} {key} = {value} → {status} (Grenze {low}–{high})"
                    send_email(subject, body)
                    _daily_sent[today].add(type_id)
                    print(f"{'🔴' if color=='Rot' else '🟠'} {body}")
                break

    # === 3. Brutzeit Beginn/Ende (nur für Brutkammer) ===
    year = datetime.now().year
    if area == "Brutkammer":
        temp_rows = df[df["key"] == "temperature"]
        if not temp_rows.empty:
            max_temp = temp_rows["value"].max()
            if max_temp >= BROOD_START_TEMP and not _brood_status.get(year, False):
                subject = "[BEEHIVE] Brutzeit beginnt wahrscheinlich"
                body = f"Temperatur ≥ {BROOD_START_TEMP}°C (max {max_temp}°C) → Brutzeitbeginn erkannt."
                send_email(subject, body)
                _brood_status[year] = True
                print(f"🔔 {body}")
            elif max_temp < BROOD_START_TEMP and _brood_status.get(year, False):
                subject = "[BEEHIVE] Brutzeit endet wahrscheinlich"
                body = f"Temperatur fiel wieder unter {BROOD_START_TEMP}°C (max {max_temp}°C) → Brutzeitende erkannt."
                send_email(subject, body)
                _brood_status[year] = False
                print(f"🔔 {body}")

def fetch_and_clean(auth_group: str, group_name: str) -> pd.DataFrame:
    c = Client()
    now = datetime.now(ZoneInfo("Europe/Berlin"))
    start = now - timedelta(minutes=5)
    all_rows = []
    entity_ids = c.get_all_entity_ids(auth_group)
    print(f"\n=== {group_name} ({auth_group}) ===")
    print(f"Gefundene Entity-IDs: {entity_ids}")

    for eid in entity_ids:
        try:
            raw = c.get_time_series(
                entityId=eid,
                authGroup=auth_group,
                startDate=start.strftime("%d.%m.%Y"),
                startTime=start.strftime("%H:%M"),
                endDate=now.strftime("%d.%m.%Y"),
                endTime=now.strftime("%H:%M")
            )
            if isinstance(raw, dict) and any(isinstance(v, dict) for v in raw.values()):
                for key, measurements in raw.items():
                    if key.lower() == "beehiveid":
                        continue
                    all_rows.extend(c._normalize_timeseries_payload(eid, measurements))
            else:
                all_rows.extend(c._normalize_timeseries_payload(eid, raw))
        except Exception as e:
            logger.error(f"Fehler beim Abrufen Entity {eid}: {e}")

    df = pd.DataFrame(all_rows)
    df = c._to_berlin_datetime(df)
    df_clean = clean_dataframe(df)
    print(f"Bereinigt: {len(df_clean)} gültige Werte")
    return df_clean

# ============================================================
# MongoDB Wrapper
# ============================================================
class BeehiveDbClient:
    def __init__(self, collection="digitalBeehive"):
        mongo_uri = os.getenv("MONGO_URI")
        if not mongo_uri:
            raise ValueError("MONGO_URI nicht gesetzt!")
        client = MongoClient(mongo_uri)
        self.collection = client["default"][collection]

    def insert_many(self, df: pd.DataFrame):
        if df.empty:
            return
        tp = TimeParser()
        docs = tp.inject_bson_datetime(df, replace_ts=True).to_dict("records")
        for doc in docs:
            try:
                self.collection.insert_one(doc)
            except errors.DuplicateKeyError:
                continue

# ============================================================
# Hauptprogramm
# ============================================================
def main():
    log_folder = "Logs"
    os.makedirs(log_folder, exist_ok=True)
    db = BeehiveDbClient()

    for name, auth_group in [
        ("Futterkammer", FUTTERKAMMER_AUTH_GROUP),
        ("Brutkammer",   BRUTKAMMER_AUTH_GROUP),
        ("Wetterstation", WETTERSTATION_AUTHT_GROUP),
    ]:
        df = fetch_and_clean(auth_group, name)
        if df.empty:
            continue
        check_anomalies(df, name)
        db.insert_many(df)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(log_folder, f"{name.lower()}_{timestamp}.csv")
        df.to_csv(filename, index=False, sep=";", encoding="utf-8-sig")
        print(f"💾 Gespeichert: {filename}")

    # Alte CSVs älter als 7 Tage löschen
    cutoff = time.time() - 7 * 86400
    for f in os.listdir(log_folder):
        path = os.path.join(log_folder, f)
        if f.endswith(".csv") and os.path.getmtime(path) < cutoff:
            os.remove(path)
            print(f"🗑️ Alte Datei gelöscht: {f}")

    save_state()  # <-- Status nach jedem Durchlauf speichern

if __name__ == "__main__":
    load_state()
    while True:
        main()
        print("\n⏱️ Warten 5 Minuten bis zum nächsten Durchlauf...\n")
        time.sleep(300)
