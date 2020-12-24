from enum import Enum
from typing import List, Tuple, Optional
from constants import date_time_constants
from models.loader_tracking.polygon_shape import PolygonShape
from utils import converters


class LocationType(Enum):
    stockpile = 0
    hopper = 1
    breaker = 2
    unknown = -1


class LocationZone:
    """
    This class encapsulates a zone entity.
    All zones have a respective location.
    """

    def __init__(self,
                 id: int,
                 name: str,
                 polygon_shape: PolygonShape,
                 timestamp: str
                 ):
        self.id = (id if id is not None else -1)
        self.name = (name if name is not None else "")
        self.polygon = polygon_shape
        self.timestamp = converters.str_to_datetime(timestamp, date_time_constants.date_time_format)

    def is_in_zone(self, point: Tuple[float, float]) -> bool:
        """
        Detect whether a given point is inside the
        respective zone
        :param point:
        :return: whether point is inside polygon of
        the given zone
        """

        return self.polygon.point_inside_shape(point)


class Location:
    """
    This class encapsulates the location entity
    and provides all the necessary information to
    detect whether other entities are operating
    withing the specific area.
    """

    def __init__(self,
                 id: int,
                 name: str,
                 polygon_shape: PolygonShape,
                 zones: List[LocationZone],
                 type: str,
                 timestamp: str
                 ):
        self.id = (id if id is not None else -1)
        self.name = (name if name is not None else "")
        self.polygon = polygon_shape
        self.zones = zones
        self.type = converters.location_str_to_location_type(type)
        self.timestamp = converters.str_to_datetime(timestamp, date_time_constants.date_time_format)

    def is_in_location_area(self, point: Tuple[float, float]) -> bool:
        """
        Detect whether a specific point in space is within
        the location area
        :param point:
        :return: if the given point is within the location polygon
        """

        return self.polygon.point_inside_shape(point)

    def detect_zone(self, point: Tuple[float, float]) -> Optional[LocationZone]:
        """
        Detects the zone within location that the given point
        is currently located within, if any.

        This method should only be called after we are sure
        that the point is inside location polygon and should
        never return None in the real case.
        :param point:
        :return: Optionally LocationZone object
        """
        for zone in self.zones:
            if zone.is_in_zone(point):
                return zone

        return None
