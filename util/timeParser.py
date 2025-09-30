from __future__ import annotations

import pandas as pd

from datetime import datetime, timedelta, timezone 

class TimeParser():
    def inject_bson_datetime(self, df: pd.DataFrame, replace_ts: bool = False) -> pd.DataFrame:
        if df.empty or "ts" not in df.columns:
            return df

        ts_num = pd.to_numeric(df["ts"], errors="coerce")
        is_ms = ts_num.dropna().gt(1e12).any()
        sec = ts_num / (1000.0 if is_ms else 1.0)

        out = df.copy()
        out["datetime_utc"] = pd.to_datetime(sec, unit="s", utc=True)

        if replace_ts:
            out = out.drop(columns=["ts"]).rename(columns={"datetime_utc": "ts"})
        return out
