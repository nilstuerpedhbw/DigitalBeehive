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
from constants import (
    WETTERSTATION_AUTHT_GROUP,
    FUTTERKAMMER_AUTH_GROUP,
    BRUTKAMMER_AUTH_GROUP,
    THRESHOLDS,
    SWING_THRESHOLD,
    BROOD_START_TEMP
)


# UTF-8 Konsole
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

DATA_FOLDER = "data"
STATE_FILE = "state.json"


# ============================================================
# Persistenter Zustand (NEU: prev_values hinzugefügt)
# ============================================================
_daily_sent = {}       # pro Datum: { alarm_type: set(unique_keys) }
_brood_status = {}     # pro Jahr: bool
_prev_values = {}      # NEU: {(entityId, key): {"value":X, "time":datetime}}


def load_state():
    """Zustände laden, falls vorhanden."""
    global _daily_sent, _brood_status, _prev_values

    os.makedirs(DATA_FOLDER, exist_ok=True)
    path = os.path.join(DATA_FOLDER, STATE_FILE)

    if not os.path.exists(path):
        logger.info("Kein State-File gefunden")
        return

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # daily_sent: dict[str, dict[str, list[list]]]
        _daily_sent = {
            datetime.strptime(day, "%Y-%m-%d").date(): {
                alarm_type: {tuple(x) for x in entries}
                for alarm_type, entries in types.items()
            }
            for day, types in data.get("daily_sent", {}).items()
        }

        _brood_status = {int(k): bool(v) for k, v in data.get("brood_status", {}).items()}

        # prev_values: {(entity,key): {"value":float, "time":str}}
        _prev_values = {
            tuple(k.split("||")): {
                "value": v["value"],
                "time": datetime.fromisoformat(v["time"])
            }
            for k, v in data.get("prev_values", {}).items()
        }

        logger.info(f"State aus {path} geladen")
    except Exception as e:
        logger.warning(f"Fehler beim Laden des State-Files: {e}")

def cleanup_old_days(keep_days: int = 2):
    """
    Entfernt alte Zeiträume aus _daily_sent und optional anderen State-Variablen.
    Nur die letzten 'keep_days' Tage bleiben erhalten.
    Standard: heute + gestern.
    """
    global _daily_sent, _prev_values

    today = datetime.now().date()

    # Erlaubte Tage: heute, gestern
    allowed = { today - timedelta(days=i) for i in range(keep_days) }

    # --- 1. daily_sent bereinigen ---
    old_days = [d for d in _daily_sent.keys() if d not in allowed]
    for d in old_days:
        del _daily_sent[d]

    # --- 2. previous values bereinigen (optional) ---
    # Wenn du previous values nur für Sensorverläufe brauchst,
    # behalten wir hier nur die Werte vom heutigen Tag.
    new_prev = {}
    for key, entry in _prev_values.items():
        dt = entry.get("time")
        if dt and isinstance(dt, datetime) and dt.date() in allowed:
            new_prev[key] = entry

    _prev_values = new_prev


def save_state():
    """Zustände dauerhaft speichern."""
    path = os.path.join(DATA_FOLDER, STATE_FILE)

    try:
        data = {
            "daily_sent": {
                str(day): {
                    alarm_type: [list(x) for x in entries]
                    for alarm_type, entries in types.items()
                }
                for day, types in _daily_sent.items()
            },
            "brood_status": _brood_status,
            "prev_values": {
                f"{eid}||{key}": {
                    "value": v["value"],
                    "time": v["time"].isoformat()
                }
                for (eid, key), v in _prev_values.items()
            }
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        logger.info(f"State gespeichert → {path}")
    except Exception as e:
        logger.error(f"Fehler beim Speichern des States: {e}")


# ============================================================
# E-Mail Versand
# ============================================================
def send_email(subject: str, body: str):
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port   = int(os.getenv("SMTP_PORT", 587))
    smtp_user   = os.getenv("SMTP_USER")
    smtp_pass   = os.getenv("SMTP_PASS")
    alert_email = os.getenv("ALERT_EMAIL")

    if not (smtp_server and smtp_user and smtp_pass and alert_email):
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
        logger.info(f"📧 E-Mail versendet: {alert_email}")
    except Exception as e:
        logger.error(f"Fehler beim E-Mail Versand: {e}")


# ============================================================
# Datenbereinigung
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
    df["beehiveIds"] = df["entityId"].map(entity_to_beehives)

    return df


# ============================================================
# Anomalieerkennung (NEU: korrekt & robust)
# ============================================================
def check_anomalies(df: pd.DataFrame, area: str):
    if df.empty:
        return

    today = datetime.now().date()
    if today not in _daily_sent:
        _daily_sent[today] = {}

    # Hilfsfunktion zum Merken
    def alert_sent(alarm_type: str, entry: tuple):
        """Prüft, ob Alarm heute schon gesendet wurde."""
        if alarm_type not in _daily_sent[today]:
            _daily_sent[today][alarm_type] = set()

        if entry in _daily_sent[today][alarm_type]:
            return True

        _daily_sent[today][alarm_type].add(entry)
        return False

    # --------------------------------------------
    # Sensorwerte prüfen
    # --------------------------------------------
    for _, row in df.iterrows():
        entityId = row["entityId"]
        key      = row["key"]
        value    = row["value"]
        dt       = row["datetime_local"]
        sensor   = row.get("sensorName", "")

        unique_key = (entityId, sensor, key)

        # ============================================
        # 1. Starke Temperaturschwankungen (NEU sauber)
        # ============================================
        pv_key = (entityId, key)

        if key == "temperature":
            prev = _prev_values.get(pv_key)

            if prev:
                diff = abs(value - prev["value"])
                minutes = (dt - prev["time"]).total_seconds() / 60.0

                if minutes <= 10 and diff >= SWING_THRESHOLD:
                    alarm_type = "temp_swing"

                    if not alert_sent(alarm_type, unique_key):
                        send_email(
                            f"[BEEHIVE] Schnelle Temperaturschwankung bei {sensor}",
                            f"ΔT = {diff:.1f}°C in {minutes:.1f} min (aktuell {value}°C)."
                        )
                        logger.info(f"🟠 Schwankung erkannt bei {unique_key}: Δ={diff:.2f}")

            # neuen Wert speichern
            _prev_values[pv_key] = {"value": value, "time": dt}

        # ============================================
        # 2. Grenzwerte prüfen
        # ============================================
        if key in THRESHOLDS.get(area, {}):
            for zone in THRESHOLDS[area][key]:
                low, high = zone["range"]
                if low <= value < high:
                    status = zone["status"]
                    color  = zone["color"]
                    alarm_type = f"threshold_{color}_{key}"

                    if not alert_sent(alarm_type, unique_key):
                        send_email(
                            f"[BEEHIVE] {status}: {area} {key} ({sensor})",
                            f"{sensor} {key} = {value} → {status} (Grenze {low}–{high})"
                        )
                        logger.info(f"{'🔴' if color=='Rot' else '🟠'} {status} bei {unique_key}")
                    break

    # ============================================
    # 3. Brutzeit (unchanged)
    # ============================================
    if area == "Brutkammer":
        year = datetime.now().year
        temps = df[df["key"] == "temperature"]

        if not temps.empty:
            max_temp = temps["value"].max()

            if max_temp >= BROOD_START_TEMP and not _brood_status.get(year, False):
                send_email(
                    "[BEEHIVE] Brutzeit beginnt",
                    f"Temperatur ≥ {BROOD_START_TEMP}°C (max {max_temp}°C)."
                )
                _brood_status[year] = True

            elif max_temp < BROOD_START_TEMP and _brood_status.get(year, False):
                send_email(
                    "[BEEHIVE] Brutzeit endet",
                    f"Temperatur unter {BROOD_START_TEMP}°C (max {max_temp}°C)."
                )
                _brood_status[year] = False


# ============================================================
# Daten abrufen + bereinigen
# ============================================================
def fetch_and_clean(auth_group: str, group_name: str) -> pd.DataFrame:
    c = Client()
    now = datetime.now(ZoneInfo("Europe/Berlin"))
    start = now - timedelta(minutes=5)

    all_rows = []
    entity_ids = c.get_all_entity_ids(auth_group)

    logger.info(f"\n=== {group_name} ({auth_group}) ===")
    logger.info(f"Gefundene Entities: {entity_ids}")

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
                for k, measurements in raw.items():
                    if k.lower() == "beehiveid":
                        continue
                    all_rows.extend(c._normalize_timeseries_payload(eid, measurements))
            else:
                all_rows.extend(c._normalize_timeseries_payload(eid, raw))

        except Exception as e:
            logger.error(f"Fehler bei Entity {eid}: {e}")

    df = pd.DataFrame(all_rows)
    df = c._to_berlin_datetime(df)
    df_clean = clean_dataframe(df)

    logger.info(f"Bereinigt: {len(df_clean)} gültige Werte")
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
    cleanup_old_days(keep_days=2)
    log_folder = "logs"
    os.makedirs(log_folder, exist_ok=True)
    db = BeehiveDbClient()

    for name, auth_group in [
        ("Futterkammer", FUTTERKAMMER_AUTH_GROUP),
        ("Brutkammer", BRUTKAMMER_AUTH_GROUP),
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
        logger.info(f"💾 CSV gespeichert: {filename}")

    # Alte Logs entfernen
    cutoff = time.time() - 7 * 86400
    for f in os.listdir(log_folder):
        path = os.path.join(log_folder, f)
        if f.endswith(".csv") and os.path.getmtime(path) < cutoff:
            os.remove(path)
            logger.info(f"🗑️ Datei gelöscht: {f}")

    save_state()


if __name__ == "__main__":
    load_state()
    cleanup_old_days(keep_days=2)
    while True:
        main()
        logger.info("\n⏱️ Warten 5 Minuten...\n")
        time.sleep(300)