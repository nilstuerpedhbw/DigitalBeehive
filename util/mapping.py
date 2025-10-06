from typing import List, Optional
from constants import (
    SENSOR_TO_ENTITY_ID,
    ENTITY_ID_TO_SENSOR,
    SENSOR_TO_BEEHIVE_IDS,
)

def _norm(name: str) -> str:
    return name.strip()

def sensor_to_entity_id(sensor_name: str) -> Optional[str]:
    return SENSOR_TO_ENTITY_ID.get(_norm(sensor_name))

def entity_id_to_sensor(entity_id: str) -> Optional[str]:
    return ENTITY_ID_TO_SENSOR.get(entity_id.strip())

def sensor_to_beehives(sensor_name: str) -> List[int]:
    return SENSOR_TO_BEEHIVE_IDS.get(_norm(sensor_name), [])

def entity_to_beehives(entity_id: str) -> List[int]:
    sensor = entity_id_to_sensor(entity_id)
    return sensor_to_beehives(sensor) if sensor else []

def beehive_has_sensor(beehive_id: int, sensor_name: str) -> bool:
    return beehive_id in set(sensor_to_beehives(sensor_name))
