import pandas as pd

from typing import List

from constants import location_constants
from models.loader_tracking.location import Location, LocationZone
from utils import converters


class LocationManager:

    def __init__(self, locations: List[Location]):
        self.locations = locations

    def update_locations_list(self,
                              zones_df: pd.DataFrame,
                              shape_polygon_df: pd.DataFrame
                              ):
        """
        Updates the zones and the shape of the location
        based on zones and shape polygon data frames
        Note: the updates are performed in-place for
        the sake of memory optimization.
        :param locations:
        :param zones_df:
        :param shape_polygon_df:
        """

        # Iterate and update location and zone shapes
        for location in self.locations:
            loc_shape_df = shape_polygon_df[shape_polygon_df[location_constants.location_shape_id] == location.id]
            loc_shape_polygon = converters.shape_df_to_polygon(loc_shape_df)
            location.polygon = loc_shape_polygon

            # Extract information about zones for the corresponding location
            loc_zones_df = zones_df[zones_df[location_constants.location_id] == location.id]
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

            location.zones = location_zones

