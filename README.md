# DigitalBeehive

## 1. Projektbeschreibung

In diesem Projekt wird ein Bienenstock mit mehreren Sensoren überwacht.  
Die Sensoren erfassen u. a. Temperatur und Luftfeuchtigkeit an verschiedenen Positionen (z. B. Futterkammer, Brutkammer, Umgebung).

Ein Python-Dienst liest die Messwerte über eine REST-API der SmartCity-IoT-Plattform aus, normalisiert und bereinigt die Daten mit Hilfe von `pandas` und speichert sie anschließend in einer MongoDB-Datenbank. Grafana kann dann auf diese MongoDB-Collection zugreifen und die Zeitreihen visualisieren.

Der Python-Dienst läuft typischerweise als Container innerhalb eines Kubernetes-Clusters und fragt die Daten in einem festen Intervall (alle 5 Minuten) ab. Zusätzlich werden Schwellwerte überwacht und bei Auffälligkeiten E-Mail-Alerts verschickt.

---

## 2. Architektur und Datenfluss

1. Sensoren (verschiedene LoRaWAN-Geräte) senden Messwerte an die IoT-Plattform.
2. Die IoT-Plattform stellt die Daten über eine REST-API bereit.
3. `client.py` ruft diese REST-API auf, wandelt die unterschiedlichen Antwortformate in ein einheitliches tabellarisches Format um und reichert die Daten mit Metainformationen an (z. B. beehiveId).
4. `main.py` orchestriert den Ablauf:
   - ruft `client.py` auf,
   - bereinigt und transformiert die Daten mit `pandas`,
   - führt ein Mapping von Entity-IDs auf Sensoren und Bienenstock-IDs durch,
   - führt eine Anomalieerkennung mit Schwellwerten aus und versendet ggf. E-Mails,
   - schreibt die Daten mit `pymongo` nach MongoDB.
5. In MongoDB werden die Messwerte in der Datenbank `default` in der Collection `digitalBeehive` gespeichert (Time-Series-ähnliche Struktur).
6. Grafana wird so konfiguriert, dass es die Daten aus dieser Collection als Zeitreihen-Dashboards visualisiert.

---

## 3. Wichtige Module und Skripte

### 3.1 `client.py` – API-Client und Normalisierung

`client.py` kapselt die gesamte Kommunikation mit der SmartCity-IoT-API.

Wesentliche Punkte:

- Aufbau eines `requests.Session` mit Retry-Logik (`Retry`, `HTTPAdapter`), um temporäre Fehler (429, 5xx) abzufangen.
- Nutzung des API-Keys aus der `.env`-Datei (`API_KEY`).
- Basis-URL:  
  `https://apis.smartcity.hn/bildungscampus/iotplatform/digitalbeehive/v1`

#### Funktionen:

- `get_all_entities(authGroup: str)`  
  Holt alle Entities einer AuthGroup und gibt die Roh-JSON-Struktur zurück.

- `get_all_entity_ids(authGroup: str) -> list[str]`  
  Extrahiert aus `get_all_entities` eine Liste der `entityId`s.

- `_get_all_time_series_keys(authGroup: str) -> list[str]`  
  Fragt alle verfügbaren Messgrößen (Time-Series Keys) ab, die später in den API-Call für die Zeitreihen übernommen werden.

- `_parse_to_unix_ts(date_str, time_str) -> int`  
  Konvertiert Datum/Uhrzeit im Format `TT.MM.JJJJ` und `HH:MM` in einen Unix-Timestamp in Millisekunden (Zeitzone Europe/Berlin).

- `get_time_series(...)`  
  Ruft die Zeitreihen für eine Entity in einem gegebenen Zeitraum ab (Start/Ende, Datum + Uhrzeit).  
  Besonderheiten:
  - Ermittelt alle relevanten Keys über `_get_all_time_series_keys`.
  - Ruft `/valueType/timeseries` auf.
  - Versucht, eine `beehiveId` per Mapping (siehe `mapping.py`) zu setzen.

- `_normalize_timeseries_payload(entity_id, payload) -> list[dict]`  
  Vereinheitlicht unterschiedliche API-Antwortstrukturen in eine flache Liste von Zeilen. Unterstützt u. a.:
  - A) `{ key: [ {ts:..., value:...}, ... ], key2: [...] }`
  - B) `{ key: {ts:..., value:...}, ... }`
  - C) `[ {key:..., ts:..., value:...}, ... ]`  
  Ergebnis: eine Liste von Dictionaries mit den Feldern:
  - `entityId`, `key`, `ts`, `value`.

- `_to_berlin_datetime(df: pd.DataFrame) -> pd.DataFrame`  
  Erkennt automatisch, ob `ts` Sekunden oder Millisekunden enthält, wandelt in UTC und anschließend in die Zeitzone `Europe/Berlin`.  
  Ergänzt eine Spalte `datetime` mit tz-aware `datetime64`-Werten.

- `_get_day_df(authGroup, day_str)` und die Convenience-Funktionen  
  - `get_today_time_series_for_all_entities`
  - `get_yesterday_time_series_for_all_entities`
  - `get_time_series_for_all_entities_on(...)`  
  Diese Funktionen laden für einen Tag bzw. ein Datum die Time-Series für alle Entities der AuthGroup, normalisieren diese, mappen die `entityId` auf `beehiveId` und liefern ein `pandas.DataFrame` zurück.

Die gesamte Normalisierung und Zeitbehandlung basiert auf `pandas`. `client.py` liefert damit eine einheitliche tabellarische Basis für die weitere Verarbeitung.

---

### 3.2 `constants.py` – Auth-Gruppen, Mappings und Schwellenwerte

`constants.py` enthält Konfigurationen und Domänenwissen:

- Auth-Gruppen:
  - `WETTERSTATION_AUTHT_GROUP`
  - `FUTTERKAMMER_AUTH_GROUP`
  - `BRUTKAMMER_AUTH_GROUP`

- Mappings zwischen Sensoren und Entities:
  - `SENSOR_TO_ENTITY_ID`
  - `ENTITY_ID_TO_SENSOR`
  - `SENSOR_TO_BEEHIVE_IDS` (welche Beuten ein Sensor abdeckt)
  - `SENSOR_TYPE` (Sensor-Typ je Sensor-ID)

- Schwellenwerte für Temperatur und Luftfeuchtigkeit:
  - `THRESHOLDS["Futterkammer"]["temperature" | "humidity"]`
  - `THRESHOLDS["Brutkammer"]["temperature"]`  
  Die Schwellenwerte sind in Kategorien wie „Kritisch“, „Beobachten“, „In Ordnung“ mit Wertebereichen und Farben (z. B. „Rot“, „Grün“) unterteilt und werden in der Anomalieerkennung verwendet.

- Anomalie-Parameter:
  - `SWING_THRESHOLD` – z. B. Temperaturänderung > 3 °C in 10 Minuten.
  - `BROOD_START_TEMP` – Temperatur, ab der Brutzeit angenommen wird.

---

### 3.3 `util/mapping.py` – Mapping von Entity-ID, Sensor und Beute

Dieses Modul kapselt das Mapping zwischen Entities, Sensoren und Bienenstöcken:

- `sensor_to_entity_id(sensor_name: str) -> Optional[str]`
- `entity_id_to_sensor(entity_id: str) -> Optional[str]`
- `sensor_to_beehives(sensor_name: str) -> List[int]`
- `entity_to_beehives(entity_id: str) -> List[int]`
- `beehive_has_sensor(beehive_id: int, sensor_name: str) -> bool`

Damit können die Rohdaten aus der IoT-Plattform (`entityId`) auf:
- den physikalischen Sensor und
- eine oder mehrere `beehiveId`s
abgebildet werden. Dieses Mapping wird sowohl in `client.py` als auch in der Bereinigung / Anomalieerkennung genutzt.

---

### 3.4 `util/timeParser.py` – Zeitstempel-Konvertierung für MongoDB

`TimeParser` stellt eine zentrale Funktion zur Verfügung:

- `inject_bson_datetime(df: pd.DataFrame, replace_ts: bool = False) -> pd.DataFrame`

Funktionalität:

- Konvertiert die Spalte `ts` in eine UTC-`datetime`-Spalte.
- Erkennt automatisch, ob `ts` in Sekunden oder Millisekunden vorliegt.
- Wenn `replace_ts=True`:  
  - wird `ts` entfernt und durch eine `ts`-Spalte als `datetime` (UTC) ersetzt, ideal für MongoDB-Time-Series.
- Wenn `replace_ts=False`:  
  - bleibt `ts` erhalten und es wird zusätzlich `datetime_utc` eingefügt.

Dieses Modul sorgt dafür, dass die in MongoDB gespeicherten Daten BSON-kompatible Zeitstempel enthalten.

---

### 3.5 `db/beehiveDbClient.py` – MongoDB-Wrapper

`BeehiveDbClient` in `db/beehiveDbClient.py` kapselt den Zugriff auf MongoDB:

- Verwendet `MONGO_URI` aus der Umgebung.
- Nutzt standardmäßig die Datenbank `default` und die Collection `digitalBeehive`.
- Optionale Time-Series-Verarbeitung über `TimeParser` (`isTimeSeries=True`).

Wichtige Funktionen:

- Konstruktor:
  - baut die Verbindung auf,
  - erstellt einen Unique-Index auf `(entityId, key, ts)`, um Duplikate zu verhindern.

- `insert_many(df: pd.DataFrame) -> Dict[str, int]`:
  - wandelt bei `isTimeSeries=True` den DataFrame mit `TimeParser` um,
  - fügt alle Dokumente ein,
  - zählt dabei `inserted`, `duplicates`, `errors`.

- `insert_one(entry: dict) -> bool`:
  - fügt ein einzelnes Dokument ein,
  - gibt `False` bei Duplikat oder Fehler zurück.

Zusätzlich gibt es allgemeine Update-Hilfsfunktionen (`update_add_field_all`, `update_many_set`, `rename_field`, etc.), um Felder in allen Dokumenten nachträglich anzupassen.

Hinweis: In `main.py` ist eine vereinfachte Version von `BeehiveDbClient` direkt definiert, die ebenfalls `pymongo` nutzt. `db/beehiveDbClient.py` stellt eine weiterentwickelte, allgemeiner einsetzbare Variante dar.

---

### 3.6 `main.py` – Orchestrierung, Bereinigung, Anomalieerkennung, Speicherung

`main.py` ist der zentrale Einstiegspunkt des Dienstes.

Hauptaufgaben:

1. Initialisierung:
   - Laden der `.env`-Variablen (`load_dotenv()`),
   - Logging-Konfiguration,
   - Laden eines persistenten Zustands aus `data/state.json` (Alarme und letzte Werte).

2. Periodischer Ablauf (Endlosschleife):
   - Alle 5 Minuten:
     - Daten für jede AuthGroup (Futterkammer, Brutkammer, Wetterstation) holen,
     - Daten bereinigen und anreichern,
     - Anomalien prüfen und ggf. E-Mail-Alerts senden,
     - Daten in MongoDB speichern,
     - Daten zusätzlich als CSV im Ordner `logs/` ablegen,
     - veraltete Logs und State-Einträge aufräumen.

3. Datenabruf und Bereinigung:
   - `fetch_and_clean(auth_group, group_name)`:
     - nutzt `Client()` aus `client.py`,
     - ruft für die letzten 5 Minuten die Time-Series pro Entity ab,
     - normalisiert die API-Antworten zu Zeilen (`_normalize_timeseries_payload`),
     - wandelt über `_to_berlin_datetime` Timestamps in tz-aware Datumswerte,
     - ruft `clean_dataframe(df)` auf.

   - `clean_dataframe(df)`:
     - entfernt Zeilen ohne sinnvolle `key` oder `value`,
     - konvertiert `value` zu numerischen Werten,
     - injiziert `datetime_utc` mittels `TimeParser`,
     - erzeugt `datetime_local` (Europe/Berlin),
     - mappt:
       - `entityId` → `sensorName` (`entity_id_to_sensor`),
       - `entityId` → `beehiveIds` (`entity_to_beehives`).

   Damit entsteht ein konsistenter DataFrame, der direkt in MongoDB gespeichert werden kann.

4. Anomalieerkennung:
   - `check_anomalies(df, area)`:
     - prüft für jede Zeile Grenzwerte gemäß `THRESHOLDS` (bereichsspezifisch: Futterkammer, Brutkammer),
     - erkennt starke Temperaturschwankungen innerhalb von 10 Minuten (`SWING_THRESHOLD`),
     - überwacht Brutbeginn/-ende in der Brutkammer (`BROOD_START_TEMP`),
     - versendet E-Mails über `send_email(...)`, falls ein Alarm erstmals an einem Tag auftritt,
     - verhindert Mehrfach-Alarme pro Tag durch einen persistenten State (`_daily_sent`, `_prev_values`, `_brood_status`, gespeichert in `data/state.json`).

5. MongoDB-Speicherung:
   - Die in `main.py` eingebaute Klasse `BeehiveDbClient`:
     - liest `MONGO_URI` aus der Umgebung,
     - schreibt in `default.digitalBeehive`,
     - nutzt `TimeParser` und `pymongo` zur Speicherung,
     - behandelt Duplikate via `DuplicateKeyError`.

6. Persistenter Zustand:
   - `load_state()`, `save_state()`:
     - speichern/lesen Alarmzustände und letzte Messwerte als JSON-Datei im Ordner `data/`,
     - `cleanup_old_days()` löscht alte Einträge (Standard: nur heute und gestern bleiben erhalten).

---

## 4. Repository-Struktur (Auszug)

Wichtige Ordner und Dateien:

- `main.py`  
  Zentrales Skript für Abruf, Bereinigung, Anomalieerkennung, Speicherung, Logging und Scheduling.

- `client.py`  
  API-Client für die IoT-Plattform, inklusive Normalisierung der Time-Series-Daten.

- `constants.py`  
  Konfiguration der Auth-Gruppen, Mappings und Schwellwerte.

- `util/mapping.py`  
  Mappings zwischen Entity-ID, Sensor und Bienenstock-IDs.

- `util/timeParser.py`  
  Konvertierung von Unix-Timestamps in BSON-kompatible `datetime`-Felder.

- `db/beehiveDbClient.py`  
  Allgemeiner MongoDB-Client mit Index-Erstellung und Insert-Statistik.

- `data/`  
  Persistenter Zustand der geholten Daten 

- `logs/`  
  Logging Datein

- `k8s/`  
  Kubernetes-Manifeste

- `requirements.txt`  
  Python-Abhängigkeiten

---

## 5. Lokale Ausführung

### 5.1 Installation

```bash
git clone https://github.com/slashcatdev/DigitalBeehive.git
cd DigitalBeehive

python -m venv .venv
# Windows:
# .venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

pip install -r requirements.txt
```

### 5.2 Start des Pollers

Vor dem Start müssen die Umgebungsvariablen (siehe Abschnitt Konfiguration) gesetzt sein.

```bash
python main.py
```

Der Dienst läuft dann in einer Endlosschleife und verarbeitet alle 5 Minuten neue Daten.

---

## 6. Konfiguration (.env)

Die wichtigsten Umgebungsvariablen werden über eine `.env`-Datei gesetzt (im Projektroot):

```env
# API-Zugriff
API_KEY=dein_api_key

# MongoDB
MONGO_URI=mongodb://user:pass@host:27017

# E-Mail-Alerting
SMTP_SERVER=smtp.example.org
SMTP_PORT=587
SMTP_USER=user@example.org
SMTP_PASS=geheimes_passwort
ALERT_EMAIL=zieladresse@example.org
```

Diese Werte werden von `dotenv` geladen und in `client.py`, `main.py` und `db/beehiveDbClient.py` verwendet.

---

## 7. Betrieb in Docker / Kubernetes

### 7.1 Docker-Image bauen

```bash
docker build -t digitalbeehive:latest .
```

Das erzeugte Image kann in eine Container-Registry (z. B. Docker Hub) gepusht werden, auf die der Kubernetes-Cluster zugreifen kann.

### 7.2 Deployment im Kubernetes-Cluster

Typischer Ablauf:

1. Secrets und ConfigMaps für:
   - `API_KEY`
   - `MONGO_URI`
   - SMTP-Parameter
   erstellen.
2. Die Manifeste aus `k8s/` anpassen.
3. Deployments und Services ausrollen:

   ```bash
   kubectl apply -f k8s/
   ```

Der Pod führt dann `main.py` aus, pollt alle 5 Minuten die IoT-API und schreibt in die konfigurierte MongoDB.

---

## 8. Nutzung der Daten in Grafana

Die von diesem Dienst erzeugten Daten liegen in MongoDB in einer Time-Series-ähnlichen Struktur vor, typischerweise mit Feldern wie:

- `entityId`
- `key` (z. B. `temperature`, `humidity`)
- `value`
- `ts` bzw. `datetime_utc`/`datetime_local`
- `sensorName`
- `beehiveIds`

Zur Visualisierung in Grafana:

1. Eine MongoDB-Datenquelle konfigurieren (Plugin/Connector abhängig von der Umgebung).
2. Ein Dashboard mit Time-Series-Panels anlegen, die:
   - nach `ts` oder `datetime_utc` gruppieren,
   - Durchschnittswerte / Maxima / Minima pro Zeitintervall aggregieren,
   - ggf. nach `beehiveIds` oder `sensorName` filtern.

Damit entsteht eine durchgängige Pipeline von den physischen Sensoren über die API, den Python-Poller, MongoDB bis hin zu den Grafana-Dashboards.
