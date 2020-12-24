import datetime
from typing import Dict

import utm
import pandas as pd

from constants import location_constants, date_time_constants
from constants.loader_constants import loader_capacity_id, loader_capacity_val
from models.loader_tracking.loader_track import LoaderStatus
from models.loader_tracking.location import LocationType
from models.loader_tracking.polygon_shape import PolygonShape


def str_to_datetime(datetime_str: str, pattern: str):
    """
    Converts given string to a Datetime object
    using specified pattern
    :param datetime_str: string that holds the date and time info
    :param pattern: pattern that describes how date and time
    are stored in the datestr
    :return: datetime object
    """
    return datetime.datetime.strptime(datetime_str, pattern)


def location_str_to_location_type(location_type: str) -> LocationType:
    """
    Converts a string that represents location type to
    a respective num
    :param location_type:
    :return:
    """
    location_type = location_type.lower().strip()

    if location_type == "stockpile":
        return LocationType.stockpile
    elif location_type == "hopper":
        return LocationType.hopper
    elif location_type == "breaker":
        return LocationType.breaker
    else:
        return LocationType.unknown


def shape_df_to_polygon(shape_df: pd.DataFrame) -> PolygonShape:
    """
    Takes a dataframe that corresponds to a single polygon
    and transforms it into PolygonShape object
    :param shape_df: dataframe with points sequence
    :return:
    """

    # Ensure that data points are sorted in ascending format
    shape_df = shape_df.sort_values(by=location_constants.shape_sequence,
                                    ascending=True)
    # Create a placeholder to add points
    points = []

    # Iterate over polygonal sequence and extract points
    for index, row in shape_df.iterrows():
        # Extract UTM locations
        easting, northing = row[location_constants.shape_easting], row[location_constants.shape_northing]

        # Convert UTM to lat/long and add to list of points
        points.append((utm.to_latlon(easting, northing,
                                     date_time_constants.utm_zone, date_time_constants.utm_zone_letter)))

    return PolygonShape(points)


def status_str_to_loader_status(status_str: str) -> LoaderStatus:
    """
    Converts a string that represents loader status into
    a enum object
    :param status_str:
    :return:
    """

    if status_str == "Y":
        return LoaderStatus.active
    elif status_str == "N":
        return LoaderStatus.unactive
    else:
        return LoaderStatus.unknown


def build_capacity_dict(capacity_df: pd.DataFrame) -> Dict[str, float]:
    """
    Transforms a data frame of loader capacity information
    into a capacity dictionary.
    :param capacity_df:
    :return:
    """
    capacity_dict = {}
    for index, row in capacity_df:
        loader_id = row[loader_capacity_id]
        loader_capacity = row[loader_capacity_val]

        capacity_dict[loader_id] = loader_capacity

    return capacity_dict
