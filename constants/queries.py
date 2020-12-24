# Delivery ash measurements:
# NOTE: NOT from the scanner - Right now using the feed ash as dummy because the delivery ash scanner is not installed
from constants import io

deliv_ash = f"""
             SELECT tag
             , to_utc_timestamp(CAST(time AS timestamp), 'PST') AS TIMESTAMP
             , CAST(time AS timestamp) AS TIMESTAMP_ORIGINAL
             , quality
             , value AS ASH_CONTENT_PERCENT
             FROM dbo_wonderware_stage.tag_evo_values
             WHERE tag = '{io.ash_content_tag}' """ + \
            """
            AND time >= from_utc_timestamp(CAST('{start_datetime}' AS timestamp), 'PST')
            AND time <= from_utc_timestamp(CAST('{end_datetime}' AS timestamp), 'PST')
            """

# Feed ash measurements: (from after the blender - NOT from the scanner)
feed_ash = f"""
             SELECT tag
             , to_utc_timestamp(CAST(time AS timestamp), 'PST') AS TIMESTAMP
             , CAST(time AS timestamp) AS TIMESTAMP_ORIGINAL
             , quality
             , value AS ASH_CONTENT
             FROM dbo_wonderware_stage.tag_evo_values
             WHERE tag = '{io.ash_content_tag}' """ + \
           """
           AND time >= from_utc_timestamp(CAST('{start_datetime}' AS timestamp), 'PST')
           AND time <= from_utc_timestamp(CAST('{end_datetime}' AS timestamp), 'PST')
           """

# Hopper outgoing flow - FLOW_RATE is the instantaneous value of the coal flow exiting the hopper (in tons/hour)
hopper_outflow = f"""
             SELECT tag
             , to_utc_timestamp(CAST(time AS timestamp), 'PST') AS TIMESTAMP
             , CAST(time AS timestamp) AS TIMESTAMP_ORIGINAL
             , quality
             , value AS FLOW_RATE
             FROM dbo_wonderware_stage.tag_evo_values
             WHERE tag = '{io.hopper_outflow_tag}' """ + \
                 """
                 AND time >= from_utc_timestamp(CAST('{start_datetime}' AS timestamp), 'PST')
                 AND time <= from_utc_timestamp(CAST('{end_datetime}' AS timestamp), 'PST')
                 """

# FAKE!!! Hopper level - not the real one since the instrument has not been installed yet!
# Using the hopper_outflow query in order to get some reasonable timestamps for the hopper leve
hopper_level = f"""
              SELECT TIMESTAMP
                     , 100.0 AS HOPPER_LEVEL 
              FROM
             (SELECT tag
             , to_utc_timestamp(CAST(time AS timestamp), 'PST') AS TIMESTAMP
             , CAST(time AS timestamp) AS TIMESTAMP_ORIGINAL
             , quality
             , value AS FLOW_RATE
             FROM dbo_wonderware_stage.tag_evo_values
             WHERE tag = '{io.hopper_outflow_tag}' """ + \
               """
               AND time >= from_utc_timestamp(CAST('{start_datetime}' AS timestamp), 'PST')
               AND time <= from_utc_timestamp(CAST('{end_datetime}' AS timestamp), 'PST') 
               )
               """

# Equipment: GPS data of the loaders
# TODO: Probably needs to be changed to HPGPS?
equip = """SELECT CONVERT(datetime2, CAST( ((TIMESTAMP AT TIME ZONE 'Pacific Standard Time') AT TIME ZONE 'UTC') AS 
datetimeoffset ) )AS TIMESTAMP , TIMESTAMP AS TIMESTAMP_ORIGINAL , EQUIP_IDENT , NORTHING, EASTING, SPEED , 
LOCATION_SNAME FROM equip_coord_trans WHERE TIMESTAMP >= '{start_datetime}' AND   TIMESTAMP <= '{end_datetime}' """

# DEBUG: Does it need to be updated with a flag saying if the load capacity is the right one?
loader = """ SELECT
            loader_id AS LOADER_ID
            , loader_external_id AS LOADER_EXTERNAL_ID
            , site_id
            , load_tons AS LOAD_TONS
            FROM loader 
            """

location = """
    SELECT location_id, location_external_id, site_id, shape_id
    , location_sname, location_type, last_updated
    FROM location """

load = """ SELECT * FROM LOAD """

load_measurement = """
    SELECT load_measurement_id
    , load_id
    , coal_variable_id
    , CAST(value AS float) AS value
    , measurement_start, measurement_end, timestamp
    FROM LOAD_MEASUREMENT """

coal_variable = """ SELECT * FROM COAL_VARIABLE """

zone = """ SELECT zone_id, location_id, shape_id, zone_sname, last_updated FROM ZONE"""

shape_polygon = """ SELECT * FROM SHAPE_POLYGON """

shape = """ SELECT shape_id, last_updated FROM SHAPE """

location_grid = """ SELECT location_grid_id, location_id, northing, easting, last_updated FROM LOCATION_GRID """

location_grid_variable = """
    SELECT location_grid_variable_id, location_grid_id, coal_variable_id
    , CAST(value AS float) AS value, trust_factor, batch_id, timestamp
    FROM LOCATION_GRID_VARIABLE """

zone_coal_variable = """SELECT zone_coal_variable_id, zone_coal_variable_sname, variable_desc FROM ZONE_COAL_VARIABLE 
"""

zone_variable = """
    SELECT zone_variable_id, zone_id, zone_coal_variable_id, CAST(value AS float) AS value,
    batch_id, location_grid_variable_batch_id FROM ZONE_VARIABLE """

# Batches
batch = """ SELECT * FROM BATCH """

# Recommendation
recommendation = """ SELECT * FROM RECOMMENDATION"""
