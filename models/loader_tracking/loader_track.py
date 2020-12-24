from enum import Enum

import utm

from constants import date_time_constants
from utils import converters
from utils.converters import status_str_to_loader_status, str_to_datetime


# TODO: Convert northing and easting coordinates

class LoaderStatus(Enum):
    unactive = 0
    active = 1
    unknown = -1


class LoaderTrack:
    """
    This class is responsible for managing all
    information regarding the track of one Loader.

    Note: this model could be used for other vehicles
    that provide GPS data
    """

    def __init__(self,
                 id: str,
                 name: str,
                 description: str,
                 status: str,
                 capacity: float = None,
                 lat: float = None,
                 lon: float = None,
                 elevation: float = None,
                 speed: float = None,
                 timestamp: str = None,
                 ):
        self.id = id
        self.name = name
        self.description = description
        self.status = status_str_to_loader_status(status)
        self.capacity = capacity
        self.lat = lat
        self.lon = lon
        self.elevation = elevation
        self.speed = speed
        self.timestamp = str_to_datetime(timestamp, date_time_constants.date_time_format)

    def update(self,
               northing: float,
               easting: float,
               elevation: float,
               speed: float,
               timestamp: str):

        lat, lon = utm.to_latlon(easting,
                                 northing,
                                 date_time_constants.utm_zone,
                                 date_time_constants.utm_zone_letter)
        self.lat = lat
        self.lon = lon
        self.elevation = elevation
        self.speed = speed
        self.timestamp = converters.str_to_datetime(timestamp, date_time_constants.date_time_format)

