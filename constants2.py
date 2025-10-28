# =============================================================
# 🔑 Auth-Gruppen
# =============================================================
WETTERSTATION_AUTHT_GROUP = "digital_bee_hive_42-s2120"
FUTTERKAMMER_AUTH_GROUP = "digital_bee_hive_42_dragino-s31lb"
BRUTKAMMER_AUTH_GROUP = "digital_bee_hive_42_dragino-d23-lb"

# =============================================================
# 🛰️ Sensor-IDs und Beuten-Zuordnung
# =============================================================
SENSOR_TO_ENTITY_ID = {
    "LoRa-2CF7F1C0613005BC": "cb45a700-fa97-11ef-9d11-f54d6a2753bf",
    "LoRa-A8404138A188669C": "a4d4afc0-6eb6-11ef-b667-951a94d6009e",
    "LoRa-A84041CC625AE81E": "a865a130-ffde-11ef-9545-f1c19ab288c3",
    "LoRa-A8404160C85A7A7B": "6fe2a6f0-2fe0-11f0-ae5e-8797afde61b2",
    "LoRa-A84041892E5A7A68": "39182140-ffde-11ef-9545-f1c19ab288c3",
    "LoRa-A840419521864618": "efa9b480-8548-11ee-b88e-89581e0193df",
    "LoRa-A840411F645AE815": "f99dddb0-ffde-11ef-9545-f1c19ab288c3", 
}

SENSOR_TO_BEEHIVE_IDS = {
    "LoRa-2CF7F1C0613005BC": [1, 2, 3],
    "LoRa-A840411F645AE815": [1],
    "LoRa-A84041892E5A7A68": [1],
    "LoRa-A84041CC625AE81E": [2],
    "LoRa-A840419521864618": [2],
    "LoRa-A8404138A188669C": [3],
    "LoRa-A8404160C85A7A7B": [3],
}

ENTITY_ID_TO_SENSOR = {v: k for k, v in SENSOR_TO_ENTITY_ID.items()}

SENSOR_TYPE = {
    "LoRa-2CF7F1C0613005BC": "LoRaWAN SenseCAP-S2120",
    "LoRa-A8404138A188669C": "LoRaWAN Dragino-S31-LB",
    "LoRa-A84041CC625AE81E": "LoRaWAN Dragino-S31-LB",
    "LoRa-A8404160C85A7A7B": "LoRaWAN Dragino-D23-LB",
    "LoRa-A84041892E5A7A68": "LoRaWAN Dragino-D23-LB",
    "LoRa-A840419521864618": "LoRaWAN Dragino-D23-LB",
    "LoRa-A840411F645AE815": "LoRaWAN Dragino-S31-LB",  
}

# =============================================================
# 🌡️ Normale Werte (für Anomalieerkennung)
# =============================================================
# NORMAL_VALUES[bereich][sensor_key][parameter] = {Saison: (min, max)}
NORMAL_VALUES = {
    "Brutkammer": {
        "LoRa-A8404160C85A7A7B": {
            "temperature": {"Winter": (33, 36), "Frühling": (34, 36), "Sommer": (35, 37), "Herbst": (34, 36)},
            "humidity": {"Winter": (55, 65), "Frühling": (50, 60), "Sommer": (50, 60), "Herbst": (55, 65)}
        },
        "LoRa-A84041892E5A7A68": {
            "temperature": {"Winter": (32, 35), "Frühling": (33, 36), "Sommer": (34, 36), "Herbst": (33, 36)},
            "humidity": {"Winter": (50, 60), "Frühling": (50, 60), "Sommer": (50, 60), "Herbst": (50, 60)}
        },
    },
    "Futterkammer": {
        "LoRa-A8404138A188669C": {
            "temperature": {"Winter": (10, 15), "Frühling": (15, 20), "Sommer": (20, 25), "Herbst": (15, 20)},
            "humidity": {"Winter": (40, 60), "Frühling": (35, 55), "Sommer": (30, 50), "Herbst": (35, 55)}
        }
    },
    "Wetterstation": {
        "LoRa-2CF7F1C0613005BC": {
            "temperature": {"Winter": (-5, 5), "Frühling": (5, 15), "Sommer": (15, 30), "Herbst": (5, 15)},
            "humidity": {"Winter": (60, 90), "Frühling": (50, 80), "Sommer": (40, 70), "Herbst": (50, 80)},
            "pressure": {"Winter": (990, 1030), "Frühling": (995, 1035), "Sommer": (1000, 1040), "Herbst": (995, 1035)}
        }
    }
}
