# Raw Coal Blending (RCB) - common functions and connections
# Legacy code from McKinsey

# NOTE: dbutils and spark packages are solely from DataBricks and thus will
# not work without connection to the cluster

from constants import connector_constants


def get_widget_argument(arg):
    """
    Processes an external parameter in the notebook.
    :param arg:
    :return:
    """
    dbutils.widgets.text(arg, "", "")
    dbutils.widgets.get(arg)
    return getArgument(arg)


def get_secret(key, scope="application-shared", ):
    """
    Gets a reference to a string from the key vault
    :param key:
    :param scope:
    :return:
    """
    return dbutils.secrets.get(scope=scope, key=key)


def jdbc_url(hostname, port, database):
    """
    Creates an connection url to JDBC database
    using the given parameters
    :param hostname:
    :param port:
    :param database:
    :return:
    """
    return "jdbc:sqlserver://{0}:{1};database={2}".format(hostname, port, database)


def read_from_SQL(jdbcUrl, table_name, conProperties):
    """
    Reads the data from an SQL database and returns
    the result as a Spark DataFrame
    :param jdbcUrl:
    :param table_name:
    :param conProperties:
    :return:
    """
    df = spark.read.jdbc(url=jdbcUrl, table=table_name, properties=conProperties)
    return df


def write_to_SQL(jdbcUrl, conProperties, df, table_name, write_mode):
    """
    Writes a Spark DataFrame to a SQL server table
    :param jdbcUrl:
    :param conProperties:
    :param df:
    :param table_name:
    :param write_mode:
    :return:
    """
    df.write.jdbc(url=jdbcUrl, table=table_name, mode=write_mode, properties=conProperties)


def write_to_SQL_Create(jdbcUrl, conProperties, df, createNewtablename, write_mode):
    """
    Creates a named SQL server table from a Spark DataFrame
    :param jdbcUrl:
    :param conProperties:
    :param df:
    :param createNewtablename:
    :param write_mode:
    :return:
    """
    df.createOrReplaceTempView("temphvactable")
    spark.sql("drop table if exists temptable_rcb_1")
    spark.sql("create table temptable_rcb_1 as select * from temphvactable")
    spark.table("temptable_rcb_1").write.jdbc(url=jdbcUrl, table=createNewtablename, mode=write_mode,
                                              properties=conProperties)
    spark.sql("drop table temptable_rcb_1")
    return


# WENCO
def read_from_wenco(table_name):
    jdbcUrl = jdbc_url(connector_constants.wenco_hostname,
                       connector_constants.wenco_port,
                       connector_constants.wenco_database)
    return read_from_SQL(jdbcUrl, table_name, connector_constants.wenco_properties())


# SHADOW DB
def read_from_shadowdb(table_name):
    jdbcUrl = jdbc_url(connector_constants.wenco_hostname,
                       connector_constants.wenco_port,
                       connector_constants.shadow_database)
    return read_from_SQL(jdbcUrl, table_name, connector_constants.wenco_properties())


# RCB
def read_from_rcb(table_name):
    jdbcUrl = jdbc_url(connector_constants.rcb_hostname,
                       connector_constants.rcb_port,
                       connector_constants.rcb_database)
    return read_from_SQL(jdbcUrl, table_name, connector_constants.rcb_properties())


def write_to_rcb(df, table_name, write_mode="overwrite"):
    jdbcUrl = jdbc_url(connector_constants.rcb_hostname,
                       connector_constants.rcb_port,
                       connector_constants.rcb_database)
    return write_to_SQL(jdbcUrl, connector_constants.rcb_properties(), df, table_name, write_mode)


# HISTORIAN
def read_from_historian(query):
    df = spark.sql(query)
    return df


# ADLS
def read_from_csv(filename, header='true', delimiter=','):
    """
    Reads CSV data from ADLS as a Spart DataFrame
    :param filename:
    :param header:
    :param delimiter:
    :return:
    """
    return spark.read.option("header", header).option("delimiter", delimiter).csv(connector_constants.mount_point
                                                                                  + filename + ".csv")


def read_from_json(filename):
    """
    Reads JSON data from ADLS as a Spark DataFrame
    :param filename:
    :return:
    """
    return spark.read.option("multiline", "true").json(connector_constants.mount_point + filename + ".json")


def read_from_parquet(filename):
    """
    Reads Parquet raw data as a Spark DataFrame
    :param filename:
    :return:
    """
    return spark.read.parquet(connector_constants.mount_point + filename + ".parquet")


def write_to_blob_storage(val, location):
    fileName = location.split('/')[-1]
    path = "/".join(location.split('/')[:-1])

    if fileName not in [file.name for file in dbutils.fs.ls(path)]:
        dbutils.fs.put(location, val)

    else:
        print("INFO: File already exists, over-writing file " + location)
        try:
            dbutils.fs.rm(location)
            dbutils.fs.put(location, val)
        except:
            print("ERROR: Something went wrong while writing file, the existing file may have been deleted.")