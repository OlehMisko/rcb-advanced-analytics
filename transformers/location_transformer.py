import pandas as pd

from constants import location_constants
from models.loader_tracking.location import Location, LocationZone
from typing import List
from utils import converters


class LocationTransformer:
    """
    This class is responsible for transforming
    the data from multiple tables (pandas DataFrames)
    into Location entities.
    """

    @staticmethod
    def transform_df_to_location_list(location_df: pd.DataFrame,
                                      zones_df: pd.DataFrame,
                                      shape_polygon_df: pd.DataFrame) -> List[Location]:
        """
        This method takes three dataframes and populates the information
        into a list of Location entities
        :param location_df:
        :param zones_df:
        :param shape_polygon_df:
        :return:
        """
        location_list = []

        for index, row in location_df.iterrows():
            # Extract main data about the location
            id = row[location_constants.location_id]
            name = row[location_constants.location_name]
            loc_type = row[location_constants.location_type]
            timestamp = row[location_constants.location_timestamp]
            loc_shape_id = row[location_constants.location_shape_id]

            # Extract sequence of points that define location polygon shape
            loc_shape_df = shape_polygon_df[shape_polygon_df[location_constants.location_shape_id] == loc_shape_id]
            loc_shape_polygon = converters.shape_df_to_polygon(loc_shape_df)

            # Extract information about zones for the corresponding location
            loc_zones_df = zones_df[zones_df[location_constants.location_id] == id]
            location_zones = []

            for z_index, z_row in loc_zones_df.iterrows():
                z_id = z_row[location_constants.zone_id]
                z_name = z_row[location_constants.zone_name]
                z_shape_id = z_row[location_constants.zone_shape_id]
                z_timestamp = z_row[location_constants.zone_timestamp]

                # Extract shape for each zone
                z_shape_df = shape_polygon_df[shape_polygon_df[location_constants.zone_shape_id] == z_shape_id]
                z_shape_polygon = converters.shape_df_to_polygon(z_shape_df)

                zone = LocationZone(z_id, z_name, z_shape_polygon, z_timestamp)
                location_zones.append(zone)

            # Create Location entity
            location = Location(id, name, loc_shape_polygon, location_zones, loc_type, timestamp)
            location_list.append(location)

        return location_list
