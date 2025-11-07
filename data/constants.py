
WETTERSTATION_AUTHT_GROUP = "digital_bee_hive_42-s2120"
FUTTERKAMMER_AUTH_GROUP   = "digital_bee_hive_42_dragino-s31lb"
BRUTKAMMER_AUTH_GROUP     = "digital_bee_hive_42_dragino-d23-lb"

SENSOR_TO_ENTITY_ID = {
    "LoRa-2CF7F1C0613005BC": "cb45a700-fa97-11ef-9d11-f54d6a2753bf",
    "LoRa-A8404138A188669C": "a4d4afc0-6eb6-11ef-b667-951a94d6009e",
    "LoRa-A84041CC625AE81E": "a865a130-ffde-11ef-9545-f1c19ab288c3",
    "LoRa-A8404160C85A7A7B": "6fe2a6f0-2fe0-11f0-a e8797afde61b2",
    "LoRa-A84041892E5A7A68": "39182140-ffde-11ef-9545-f1c19ab288c3",
    "LoRa-A840419521864618": "efa9b480-8548-11ee-b88e-89581e0193df",
    "LoRa-A840411F645AE815": "f99dddb0-ffde-11ef-9545-f1c19ab288c3",
}
ENTITY_ID_TO_SENSOR = {v: k for k, v in SENSOR_TO_ENTITY_ID.items()}

THRESHOLDS = {
    "Futterkammer": {
        "temperature": [
            {"status": "Kritisch",   "range": (-999,   5), "color": "Rot"},
            {"status": "Beobachten", "range": (   5,  15), "color": "Orange"},
            {"status": "In Ordnung","range": (  15,  35), "color": "Grün"},
            {"status": "Beobachten", "range": (  35,  40), "color": "Orange"},
            {"status": "Kritisch",   "range": (  40, 999), "color": "Rot"},
        ],
        "humidity": [
            {"status": "Kritisch",   "range": (-999,   40), "color": "Rot"},
            {"status": "Beobachten", "range": (   40,  50), "color": "Orange"},
            {"status": "In Ordnung","range": (  50,  70), "color": "Grün"},
            {"status": "Beobachten", "range": (  70,  80), "color": "Orange"},
            {"status": "Kritisch",   "range": (  80, 999), "color": "Rot"},
        ],
    },
    "Brutkammer": {
        "temperature": [
            {"status": "Kritisch",   "range": (-999,   32), "color": "Rot"},
            {"status": "Beobachten", "range": (   32, 33.5), "color": "Orange"},
            {"status": "In Ordnung","range": ( 33.5, 35.5), "color": "Grün"},
            {"status": "Beobachten", "range": ( 35.5,   36), "color": "Orange"},
            {"status": "Kritisch",   "range": (   36, 999), "color": "Rot"},
        ]
    }
}

SWING_THRESHOLD = 3.0  # z. B. Temperatur-Änderung >3°C in 10 Minuten zählt als große Schwankung
BROOD_START_TEMP = 30.0  # z. B. erste Überschreitung → Brutzeit wahrscheinlich
