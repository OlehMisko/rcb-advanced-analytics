from constants.io import equipment_table, equipment_coords_table
from utils.connection import read_from_wenco


class LoaderConnector:

    @staticmethod
    def load_basic_equipment_info():
        """
        Loads the data from WENCO database
        and selects
        :return:
        """
        return read_from_wenco(equipment_table).toPandas()

    @staticmethod
    def load_equipment_location_info(limit: int = 100):
        # TODO: Add logic to load only for loaders as we know Loaders ids
        # TODO: Add logic to load one data point (the most recent) for each equipment record
        # TODO: Remove limit as it is only for debug purposes.
        equip_coord_trans = read_from_wenco(equipment_coords_table).limit(limit)
        return equip_coord_trans.toPandas()

