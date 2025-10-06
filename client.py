from __future__ import annotations

import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

from constants import WETTERSTATION_AUTHT_GROUP
from util.timeParser import TimeParser
from util.mapping import entity_to_beehives

BASE_URL = "https://apis.smartcity.hn/bildungscampus/iotplatform/digitalbeehive/v1"   
API_KEY  = os.getenv("API_KEY")

class Client():
    def _make_session(self) -> requests.Session:
        s = requests.Session()
        retries = Retry(
            total=3, backoff_factor=0.3,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET","POST","PUT","DELETE","PATCH"])
        )
        s.mount("https://", HTTPAdapter(max_retries=retries))
        s.mount("http://",  HTTPAdapter(max_retries=retries))
        s.request = lambda *a, **kw: requests.Session.request(s, *a, **kw)
        return s 

    def _normalize_timeseries_payload(self, entity_id: str, payload) -> list[dict]:
        """
        Normalisiert typische Formen auf Zeilen:
        Erwartete Varianten:
          A) { key: [ {ts:..., value:...}, ... ], key2: [...] }
          B) { key: {ts:..., value:...}, ... }
          C) [ {key:..., ts:..., value:...}, ... ] (seltener)
        """
        rows: list[dict] = []

        # Variante C: Liste am Top-Level
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    rows.append({
                        "entityId": entity_id,
                        "key": item.get("key"),
                        "ts": item.get("ts"),
                        "value": item.get("value"),
                    })
                else:
                    rows.append({"entityId": entity_id, "key": None, "ts": None, "value": item})
            return rows

        # A/B: Dict am Top-Level
        if isinstance(payload, dict):
            for metric_key, points in payload.items():
                if points is None:
                    continue
                if not isinstance(points, list):
                    points = [points]
                for p in points:
                    if isinstance(p, dict):
                        ts = p.get("ts")
                        val = p.get("value")
                    else:
                        ts = None
                        val = p
                    rows.append({
                        "entityId": entity_id,
                        "key": metric_key,
                        "ts": ts,
                        "value": val
                    })
            return rows

        # Fallback
        rows.append({
            "entityId": entity_id, "key": None, "ts": None,
            "value": f"unexpected payload type: {type(payload).__name__}"
        })
        return rows

    def _to_berlin_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "ts" not in df.columns:
            return df
        ts_num = pd.to_numeric(df["ts"], errors="coerce")
        unit = "ms" if ts_num.dropna().gt(1e12).any() else "s"
        df["datetime"] = pd.to_datetime(ts_num, unit=unit, utc=True).dt.tz_convert("Europe/Berlin")
        return df

    def _get_day_df(self, authGroup: str, day_str: str) -> pd.DataFrame:
        """
        Kernlogik: holt alle Entities und lädt deren Time-Series für den gegebenen Tag (00:00–23:59, Europe/Berlin).
        day_str: "TT.MM.JJJJ"
        """
        # Datum validieren
        try:
            datetime.strptime(day_str, "%d.%m.%Y")
        except ValueError as e:
            raise ValueError(f'Ungültiges Datum "{day_str}". Erwartet: "TT.MM.JJJJ".') from e

        rows: list[dict] = []
        entity_ids = self.get_all_entity_ids(authGroup)

        for eid in entity_ids:
            try:
                data = self.get_time_series(
                    entityId=eid,
                    authGroup=authGroup,
                    startTime="00:00",
                    startDate=day_str,
                    endTime="23:59",
                    endDate=day_str
                )

                # Manche APIs liefern eine Ebene mehr/weniger – beides abfedern:
                # Beispiel 1: data == { "temperature": [...], "humidity": [...] }
                # Beispiel 2: data == { "something": { "temperature": [...], ... } }
                if isinstance(data, dict) and any(isinstance(v, dict) for v in data.values()):
                    # Eine Ebene tiefer iterieren
                    for _, measurements in data.items():
                        rows.extend(self._normalize_timeseries_payload(eid, measurements))
                else:
                    rows.extend(self._normalize_timeseries_payload(eid, data))

            except Exception as e:
                rows.append({"entityId": eid, "key": None, "ts": None, "value": f"error: {str(e)}"})

        df = pd.DataFrame(rows)
        if "ts" in df.columns and not df.empty:
            # Leere Strings als NA behandeln und in Zahl konvertieren
            df["ts"] = pd.to_numeric(df["ts"].replace(["", "NaN", "nan"], pd.NA), errors="coerce")
            df = df[df["ts"].notna()].copy()
        df["beehiveId"] = df["entityId"].map(entity_to_beehives)
        return self._to_berlin_datetime(df)
    
    def get_all_entities(self, authGroup:str) -> json:
        session =  self._make_session()
        r = session.get(
            f"{BASE_URL}/authGroup/{authGroup}/entityId?page=0",
            headers={"x-apikey": f"{API_KEY}"}
        )
        r.raise_for_status()
        return r.json()

    def _get_all_time_series_keys(self, authGroup) -> list[str]:
        session =  self._make_session()
        r = session.get(
            f"{BASE_URL}/authGroup/{authGroup}/valueType",
            params={"x-apikey": API_KEY} 
        ) 
        r.raise_for_status()
        value_types = r.json()
        time_series_keys = [item["key"] for item in value_types["valueType"]["TIME_SERIES"]]      
        return time_series_keys
    
    def _parse_to_unix_ts(self, date_str: str, time_str: str) -> int:
            dt = datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")
            dt = dt.replace(tzinfo=ZoneInfo("Europe/Berlin"))
            ts = dt.timestamp()
            return int(ts * 1000)
    
    def get_all_entity_ids(self, authGroup:str) -> list[str]:
         entites = self.get_all_entities(authGroup)
         entity_ids = [item["entityId"]["id"] for item in entites["entities"]]
         return entity_ids
    
    def get_time_series(self,
                         entityId:str,
                         authGroup:str,
                         startTime:str="00:00",
                         startDate:str="01.01.1970",
                         endTime:str="23:59",
                         endDate:str="24.09.2025"):
        
        startTs = self._parse_to_unix_ts(startDate, startTime)
        endTs = self._parse_to_unix_ts(endDate, endTime)

        session =  self._make_session()

        keys =",".join(self._get_all_time_series_keys(authGroup)) 
        r = session.get(
            f"{BASE_URL}/authGroup/{authGroup}/entityId/{entityId}/valueType/timeseries",
            params={"x-apikey": API_KEY,
                    "keys": keys,
                    "endTs": str(endTs),
                    "startTs": str(startTs)
                     } 
        ) 
        r.raise_for_status() 

        time_series = r.json()

        try:
         beehive_id = entity_to_beehives(entityId)  # erwartet: vorhandene Mapping-Funktion
        except Exception:
         beehive_id = None

        time_series["timeseries"].setdefault("beehiveId", beehive_id)
        return time_series   
         
    def get_today_time_series_for_all_entities(self, authGroup: str) -> pd.DataFrame:
        day_str = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y")
        return self._get_day_df(authGroup, day_str)

    def get_yesterday_time_series_for_all_entities(self, authGroup: str) -> pd.DataFrame:
        day_str = (datetime.now(ZoneInfo("Europe/Berlin")) - timedelta(days=1)).strftime("%d.%m.%Y")
        return self._get_day_df(authGroup, day_str)
    
    def get_yesterday_time_series_for_all_entities_bson(self, authGroup: str, *, replace_ts: bool = True) -> pd.DataFrame:
        """
        Wie get_yesterday_time_series_for_all_entities, aber ergänzt eine BSON-geeignete UTC-Spalte.
        - replace_ts=False: fügt 'datetime_utc' hinzu
        - replace_ts=True: ersetzt 'ts' durch UTC-datetime (praktisch für direkten Mongo-Insert)
        """
        tp = TimeParser()
        df = self.get_yesterday_time_series_for_all_entities(authGroup)
        df = tp.inject_bson_datetime(df, replace_ts=replace_ts)
        return df

    def get_time_series_for_all_entities_on(self, authGroup: str, day: str) -> pd.DataFrame:
        """
        day: "TT.MM.JJJJ"
        """
        return self._get_day_df(authGroup, day)
    
if __name__ == "__main__":
    c = Client()
    c.get_yesterday_time_series_for_all_entities_bson(WETTERSTATION_AUTHT_GROUP).to_csv("test_data/bson_test.csv",index=False, sep=";", encoding="utf-8-sig")