from __future__ import annotations

from datetime import datetime, timedelta, timezone 

class TimeParser():
    def ts_to_bson_datetime(self, ts: int | float) -> datetime:
        """
        Wandelt Unix-Timestamp (ms oder s) robust in UTC-aware datetime um.
        - ms: ts > 1e12   -> /1000
        - s:  sonst
        """
        try:
            t = float(ts)
        except Exception:
            raise ValueError(f"Ungültiger Timestamp: {ts!r}")

        if t > 1e12:  # Heuristik: Millisekunden
            return datetime.fromtimestamp(t / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(t, tz=timezone.utc)