# DigitalBeehive

Kleines Python-Projekt zum Abrufen, Ablegen und Visualisieren von Sensordaten („Beehive“-Kontext). Daten fließen von einer externen API → optional CSV → MongoDB → (später) Grafana.

---

## Inhalt
- [DigitalBeehive](#digitalbeehive)
  - [Inhalt](#inhalt)
  - [Ziele](#ziele)
  - [Architektur \& Datenfluss](#architektur--datenfluss)
  - [Setup](#setup)
    - [Voraussetzungen](#voraussetzungen)
    - [Installation](#installation)
  - [Konfiguration (.env )](#konfiguration-env-)
    - [.env](#env)
  - [Grafana (später)](#grafana-später)
- [](#)
  - [Roadmap \& offene Punkte](#roadmap--offene-punkte)

---

## Ziele
- **API anbinden**, Daten verstehen und robust abrufen.
- **Datenhaltung** zunächst CSV, dann **MongoDB** (Collection `digitalBeehive` in DB `default`).
- Später: **Grafana** für Metriken und Zustandsklassifikation („normaler Zustand“).

---

## Architektur & Datenfluss
```
API → (client.py) → CSV (optional in /data) → (job.py) → MongoDB (db=default, coll=digitalBeehive) → Grafana (später)
```

---

## Setup

### Voraussetzungen
- Python == 3.11
- (Optional) MongoDB lokal/remote
- (Optional) Grafana

### Installation
```bash
git clone https://github.com/nilstuerpedhbw/DigitalBeehive.git
cd DigitalBeehive

python -m venv .venv
# Windows: .venv\Scriptsctivate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

---

## Konfiguration (.env )

### .env
Lege eine Datei `.env` im Projektroot an (Beispiel):
```
MONGO_URI=mongodb://localhost:27017
MONGO_DB=default
MONGO_COLLECTION=digitalBeehive

API_BASE_URL=https://<deine-api>/...
API_KEY=<dein-key-oder-token>
```

## Grafana (später)
- Datenquelle: **MongoDB** (Plugin/Connector).  
- Panel-Typ: **Time series** (Temperaturen, Feuchte etc.).  
- Query: Aggregation nach Zeitfenster (`$group` auf Timestamp, `avg`/`min`/`max`).

---

#
## Roadmap & offene Punkte

**Kurzfristig**
- [ ] `job.py` → direkte MongoDB-Writes testen
- [ ] Raspberry Pi systemd-Timer aktivieren
- [ ] Erstes Grafana-Dashboard anlegen

**Langfristig**
- [ ] „Normalzustand“ fachlich definieren und als Regeln/Heuristiken implementieren
- [ ] Validierung mit Echtdaten (Labels, Threshold-Tuning)

**Offene Fragen**
- Entities/AuthGroups/TempC1C2C3 – Datendefinitionen klären  
- Timestamps/Time Series in MongoDB (Schema & Indexe)

---
