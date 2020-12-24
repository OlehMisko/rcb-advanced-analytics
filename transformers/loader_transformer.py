from typing import List, Dict

import pandas as pd

from constants.loader_constants import *
from models.loader_tracking.loader_track import LoaderTrack
from utils import converters


class LoaderTransformer:
    """
    This class is responsible for initial Loader population
    based on the information about current site equipment.
    """

    @staticmethod
    def transform_df_into_loader_tracks_list(equip_df: pd.DataFrame,
                                             capacity_df: pd.DataFrame) -> List[LoaderTrack]:
        """
        Transforms a data frame of equipment into a list of LoaderTrack
        entities.
        Additionally leverages capacity DataFrame to add the capacity
        feature to the model.
        :param equip_df:
        :param capacity_df:
        :return:
        """
        # Extract information only about loaders
        loaders_df = equip_df[equip_df[loader_model_number_col] == loader_model_query_res]
        loaders = []
        capacity_dict = converters.build_capacity_dict(capacity_df)

        # Extract only relevant columns
        loaders_df = loaders_df[loader_model_cols]

        # Populate models based on the DataFrame
        for index, row in loaders_df.iterrows():
            loader_capacity = capacity_dict[row[loader_name]] if row[loader_name] in capacity_dict.keys() else 0.0

            loaders.append(LoaderTrack(row[loader_equip_id],
                                       row[loader_name],
                                       row[loader_desc],
                                       row[loader_status],
                                       loader_capacity))

        return loaders




