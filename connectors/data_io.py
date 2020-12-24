# Raw Coal Blending (RCB) - common functions and connections
# Legacy code from McKinsey

import pandas as pd
import pyspark.sql.functions as F

from constants import io, queries, aliases
from utils.connection import read_from_rcb, read_from_wenco, read_from_historian, write_to_rcb


class LoadFunc:
    """
    Helper functions for loading the data
    """

    def __init__(self, now):
        self.now = now

    def kwargs_convert_relative_to_absolute_time(self, kwargs):
        # If present, from_hour and to_hour override start_datetime and end_datetime, respectively
        if kwargs['from_hour'] is not None:
            kwargs['start_datetime'] = pd.to_datetime(self.now) + pd.to_timedelta(kwargs['from_hour'], unit='hours')
        if kwargs['to_hour'] is not None:
            kwargs['end_datetime'] = pd.to_datetime(self.now) + pd.to_timedelta(kwargs['to_hour'], unit='hours')
        return kwargs

    @staticmethod
    def kwargs_convert_assumed_utc_to_tz(kwargs, time_fields_list, to_tz):
        # Convert dates to 'to_tz' timezone
        for k in time_fields_list:
            kwargs[k] = pd.to_datetime(kwargs[k]).tz_localize('UTC').tz_convert(to_tz).tz_localize(None).round('1s')
        return kwargs

    @staticmethod
    def convert_decimal_cols_to_float(df):
        decimal_cols_list = [c for c, dtype in df.dtypes if dtype[0:7] == 'decimal']
        cols_list = [c if c not in decimal_cols_list else F.col(c).cast('float').alias(c) for c in df.columns]
        return df.select(cols_list)

    @staticmethod
    def make_all_column_names_capital(df):
        cols_list = [F.col(c).alias(c.upper()) for c in df.columns]
        return df.select(cols_list)

    @staticmethod
    def ash_postprocess(df):
        df = df.withColumn('TIMESTAMP', F.to_utc_timestamp(F.col('TIMESTAMP_ORIGINAL'),
                                                           'Canada/Pacific'))  # 'Canada/Pacific' or 'PST' ?
        return df

    def basic_time_filter_tz_canada_pt(self, query,
                                       start_datetime=io.beginning_of_time, end_datetime=io.end_of_time,
                                       from_hour=None, to_hour=None, **kw):
        kwargs = dict(start_datetime=start_datetime, end_datetime=end_datetime, from_hour=from_hour, to_hour=to_hour,
                      **kw)
        kwargs = self.kwargs_convert_relative_to_absolute_time(kwargs)
        kwargs = self.kwargs_convert_assumed_utc_to_tz(kwargs, ['start_datetime', 'end_datetime'],
                                                       'Canada/Pacific')  # 'Canada/Pacific' or 'PST' ?
        return query.format(**kwargs)

    def basic_time_filter_tz_utc(self, query,
                                 start_datetime=io.beginning_of_time, end_datetime=io.end_of_time,
                                 from_hour=None, to_hour=None, **kw):
        kwargs = dict(start_datetime=start_datetime, end_datetime=end_datetime, from_hour=from_hour, to_hour=to_hour,
                      **kw)
        kwargs = self.kwargs_convert_relative_to_absolute_time(kwargs)
        return query.format(**kwargs)

    def simple(self, query, **kwargs):
        return query.format(**kwargs)


class SaveFunc:
    """
    Helper functions for saving the data
    """

    @staticmethod
    def convert_to_spark_dataframe_with_correct_datatypes(df_pandas, schema):
        """ Converts a pandas dataframe into a spark dataframe and
    solves the problem of converting numbers form floats in pandas to decimal in spark """
        df = spark.createDataFrame(df_pandas)
        select_cols_list = [F.col(f.name).cast(f.dataType.simpleString()) for f in schema]
        df = df.select(select_cols_list)
        return df


class SqlTable:
    """
    Collecting the methods needed to load a table here,
    so that one table can be loaded by simply doing <table_name>.load()
    """

    def __init__(self, query_raw, query_func=None, postprocess_func=None, db='rcb',
                 append_enabled=False, autogen_key=None, alias='ALIAS'):
        self.alias = alias
        self.query_raw = query_raw
        self.query_func = query_func
        self.postprocess_func = postprocess_func
        self.db = db
        self.append_enabled = append_enabled
        self.autogen_key = autogen_key
        self.read_func = self.get_read_function()
        self.append_func = self.get_append_function()

    def load(self, **kwargs):
        query = self.get_query(**kwargs)
        loadf = self.read_func
        df = loadf(query)

        # Converting all the decimals to float to avoid problems summing floats later in the python/pandas code
        df = LoadFunc.convert_decimal_cols_to_float(df)
        # Converting all column names to capital upon loading to avoid problems with uppercase/lowercase
        df = LoadFunc.make_all_column_names_capital(df)

        if self.postprocess_func is not None:
            df = self.postprocess_func(df)
        return df

    def get_read_function(self):
        loadf_dict = {'rcb': read_from_rcb,
                      'wenco': read_from_wenco,
                      'historian': read_from_historian,
                      }
        loadf = loadf_dict.get(self.db, read_from_rcb)
        return loadf

    def get_append_function(self):
        if (not self.append_enabled) or (self.db != 'rcb'):
            return None

        def append_func(df):
            write_to_rcb(df, f'dbo.{self.alias}', 'append')

        return append_func

    def get_schema(self):
        query = self.get_query()
        loadf = self.read_func
        schema = loadf(query).schema
        return schema

    def get_append_schema(self):
        append_schema = [f for f in self.get_schema() if f.name != self.autogen_key]
        return append_schema

    def append(self, df):
        """ Append data to the table in the database.
        If the input is a pandas dataframe, converts it into a dataframe with the correct data type.
        In any case, the input dataframe must contain the right set of columns """
        appendf = self.append_func
        if appendf is None:
            print(f'Cannot append to {self.alias}: no append function')
            return
        schema = self.get_append_schema()
        if type(df) == pd.DataFrame:
            df = SaveFunc.convert_to_spark_dataframe_with_correct_datatypes(df, schema)
        appendf(df)

    def get_query(self, **kwargs):
        q = self.query_func(self.query_raw, **kwargs) if self.query_func is not None else self.query_raw
        query = f'({q})' if self.db == 'historian' else f'({q}) {self.alias}'
        return query


class DataIO:
    """Class for input output.
    All the tables are accessed fromh ere.
    Run the Utils before this in order to define read_from_wenco, read_from_historian and read_from_rcb"""

    def __init__(self, now='2020-09-14 12:27:00'):
        self.now = now
        self.functions = LoadFunc(now=now)
        self.define_tables()

    def define_tables(self):
        # Defining variables qq and ff for queries and functions in order to make the code shorter
        ff = self.functions
        self.feed_ash = SqlTable(queries.feed_ash,
                                 query_func=ff.basic_time_filter_tz_utc,
                                 db=io.historian_db_name,
                                 alias=aliases.feed_ash)

        self.deliv_ash = SqlTable(queries.deliv_ash,
                                  query_func=ff.basic_time_filter_tz_utc,
                                  db=io.historian_db_name,
                                  alias=aliases.delivered_ash)

        self.equip = SqlTable(queries.equip,
                              query_func=ff.basic_time_filter_tz_canada_pt,
                              db=io.wenco_db_name,
                              alias=aliases.equipment)

        self.loader = SqlTable(queries.loader,
                               query_func=ff.simple,
                               db=io.rcb_db_name,
                               alias=aliases.loader)

        self.hopper_outflow = SqlTable(queries.hopper_outflow,
                                       query_func=ff.basic_time_filter_tz_utc,
                                       db=io.historian_db_name,
                                       alias=aliases.hopper_outflow)

        # TODO: Redefine when instrument arrive!!!
        self.hopper_level = SqlTable(queries.hopper_level,
                                     query_func=ff.basic_time_filter_tz_utc,
                                     db=io.historian_db_name,
                                     alias=aliases.hopper_level)

        self.location = SqlTable(queries.location,
                                 query_func=ff.simple,
                                 db=io.rcb_db_name,
                                 alias=aliases.location)

        self.load = SqlTable(queries.load,
                             query_func=ff.simple,
                             db=io.rcb_db_name,
                             autogen_key='load_id',
                             append_enabled=True,
                             alias=aliases.load)

        self.load_measurement = SqlTable(queries.load_measurement,
                                         query_func=ff.simple,
                                         db=io.rcb_db_name,
                                         autogen_key='load_measurement_id',
                                         append_enabled=True,
                                         alias=aliases.load_measurement)

        self.coal_variable = SqlTable(queries.coal_variable,
                                      query_func=ff.simple,
                                      db=io.rcb_db_name,
                                      autogen_key='coal_variable_id',
                                      append_enabled=True,
                                      alias=aliases.coal_variable)
        # Shapes related
        self.shape_polygon = SqlTable(queries.shape_polygon,
                                      query_func=ff.simple,
                                      db=io.rcb_db_name,
                                      append_enabled=True,
                                      alias=aliases.shape_polygon)

        self.shape = SqlTable(queries.shape,
                              query_func=ff.simple,
                              db=io.rcb_db_name,
                              autogen_key='shape_id',
                              append_enabled=True,
                              alias=aliases.shape)

        self.location_grid = SqlTable(queries.location_grid,
                                      query_func=ff.simple,
                                      db=io.rcb_db_name,
                                      autogen_key='location_grid_id',
                                      append_enabled=True,
                                      alias=aliases.location_grid)
        # Zones- and grid-related
        self.zone = SqlTable(queries.zone,
                             query_func=ff.simple,
                             db=io.rcb_db_name,
                             autogen_key='zone_id',
                             append_enabled=True,
                             alias=aliases.zone)

        self.location_grid_variable = SqlTable(queries.location_grid_variable,
                                               query_func=ff.simple,
                                               db=io.rcb_db_name,
                                               autogen_key='location_grid_variable_id',
                                               append_enabled=True,
                                               alias=aliases.location_grid_variable)

        self.zone_coal_variable = SqlTable(queries.zone_coal_variable,
                                           query_func=ff.simple,
                                           db=io.rcb_db_name,
                                           autogen_key='zone_coal_variable_id',
                                           append_enabled=True,
                                           alias=aliases.zone_coal_variable)

        self.zone_variable = SqlTable(queries.zone_variable,
                                      query_func=ff.simple,
                                      db=io.rcb_db_name,
                                      autogen_key='zone_variable_id',
                                      append_enabled=True,
                                      alias=aliases.zone_variable)
        # Batch
        self.batch = SqlTable(queries.batch,
                              query_func=ff.simple,
                              db=io.rcb_db_name,
                              autogen_key='batch_id',
                              append_enabled=True,
                              alias=aliases.batch)
        # Recommendation
        self.recommendation = SqlTable(queries.recommendation,
                                       query_func=ff.simple,
                                       db=io.rcb_db_name,
                                       autogen_key='recommendation_id',
                                       append_enabled=True,
                                       alias=aliases.recommendation)
