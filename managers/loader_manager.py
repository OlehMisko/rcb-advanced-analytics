import pandas as pd

from typing import List

from constants.loader_constants import *
from models.loader_tracking.loader_track import LoaderTrack


class LoaderManager:
    def __init__(self, loaders: List[LoaderTrack]):
        self.loaders = loaders

    def update_loader_information(self,
                                  gps_df: pd.DataFrame):
        """
        Updates loader location, speed and elevation based
        on the data from Wenco.
        Note: the updates are performed in-place for
        the sake of memory optimization.
        :param gps_df:
        :return:
        """

        for loader in self.loaders:
            # Extract the data for this specific loader
            loader_info_df = gps_df[gps_df[loader_equip_id] == loader.id]

            # If we have information for this loader
            if not loader_info_df.empty:
                loader.update(loader_info_df[loader_location_northing],
                              loader_info_df[loader_location_easting],
                              loader_info_df[loader_location_elevation],
                              loader_info_df[loader_location_speed],
                              loader_info_df[loader_location_timestamp])
            else:
                # TODO: Add error logging that we do not have info for some Loader
                pass

    def get_loaders_list(self):
        return self.loaders
