# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Ingests data from Synapse Link CSV into Silver layer
# MAGIC Based on this pattern refactored for Databricks:
# MAGIC
# MAGIC https://github.com/arasdk/fabric-code-samples/blob/main/synapse_link/synapse_link_adls_incremental_merge.py
# MAGIC
# MAGIC https://www.linkedin.com/pulse/loading-incremental-changes-from-dynamics-365-finance-allan-rasmussen-upmdc
# MAGIC
# MAGIC
# MAGIC
# MAGIC ####Issues:
# MAGIC https://github.com/arasdk/fabric-code-samples/issues
# MAGIC |#|Description|Comment|Status
# MAGIC |-|-|-|-|
# MAGIC |Github#1 opened on May 24 by arasdk|SinkCreatedOn and SinkModifiedOn are loaded as NULL values bug synapse_link|The dateformat used for SinkCreatedOn and SinkModifiedOn in CSV files is MM/DD/YYYY HH:MM:SS AM/PM, resuling in null value|Fixed|
# MAGIC |Github#2 opened on May 24 by arasdk|Text fields ending with CRLF are imported with a whitespace+LF bug synapse_link|||
# MAGIC |Github#3 opened on May 24 by arasdk|No partitioning/partition pruning is applied on the Fabric tables enhancement synapse_link|Added Partition column to UC table. Review to make sure the SL partition is most appropriate, it may be better to repartion by source partition field if available|Fixed|
# MAGIC |Github#4 opened on May 24 by arasdk|Support for hard-deletes enhancement synapse_link|||
# MAGIC |Github#5 opened on May 24 by arasdk|Consider InitialSyncState for each table when processing changes enhancement synapse_link|Log to a table instead to maintain watermark per table and log runs and schema changes in a different table||
# MAGIC |Github#6 opened on May 24 by arasdk|Load GlobalOptionsSetMetaData enhancement synapse_link|||

# COMMAND ----------

# The path to the shortcut that points to the storage account folder where the incremental csv files are located
synapse_link_shortcut_path = "/Volumes/test_udap/raw/synapse_link_test"

# This folder is placed in the source storage and used for logs and watermarks
incremental_merge_folder = f"{synapse_link_shortcut_path}/databricks/incremental_merge_state"

# Define the batch size to control how many table merges are parallelized
batch_size = 8

# COMMAND ----------

print (incremental_merge_folder)

# COMMAND ----------

import concurrent.futures
import json
import logging

#logging.basicConfig(level=logging.INFO)
#logger = logging.getLogger(__name__)

import time
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    row_number,
    desc,
    lit,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    DoubleType,
    BooleanType,
    DecimalType,
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# changelog.info contains the active folder name
# Synapse.log contains a comma-separated list of previously processed folders ordered by date
CHANGELOG_URL = f"{synapse_link_shortcut_path}/Changelog/changelog.info"
SYNAPSELOG_URL = f"{synapse_link_shortcut_path}/Changelog/Synapse.log"

# Use dbutils to get the list of all processed folders.
# This saves having to perform a list operation on the storage account.
# Note: SYNAPSELOG folders are sorted by date, so the newest folder is last in the list
PREV_FOLDERS = dbutils.fs.head(SYNAPSELOG_URL, 1024 * 10000).split(",")

# Set up logging
# Logs will be appended to files YYYYMMDD.log files
date_str = time.strftime("%Y%m%d")
log_path = f"{incremental_merge_folder}/Logs"
file_name = f"{date_str}.log"
dbutils.fs.mkdirs(log_path)

#logging.basicConfig(
#    filename=f"{log_path}/{file_name}",
#    filemode="a",
#    level=logging.INFO,
#    format="%(asctime)s - %(levelname)s - %(message)s",
#    force=True,
#)

# Create a console handler so we can see log messages directly in Notebook output
#console_handler = logging.StreamHandler()
#console_handler.setLevel(logging.INFO)
#console_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
#console_handler.setFormatter(console_format)
#logging.getLogger().addHandler(console_handler)

# COMMAND ----------

# Utility methods
def save_last_processed_folder(folder):
    dbutils.fs.put(
        f"{incremental_merge_folder}/.last_processed_folder", folder, True
    )


def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except:
        return False
def get_last_processed_folder() -> str:
    if path_exists(f"{incremental_merge_folder}/.last_processed_folder"):
        return dbutils.fs.head(
            f"{incremental_merge_folder}/.last_processed_folder", 1024
        )
    return None


# Returns a StructField object based on the provided field name and data type.
def get_struct_field(field_name, data_type) -> StructField:
    if data_type == "string":
        return StructField(field_name, StringType(), True)
    elif data_type == "guid":
        return StructField(field_name, StringType(), True)
    elif data_type == "int64":
        return StructField(field_name, LongType(), True)
    elif data_type == "int32":
        return StructField(field_name, IntegerType(), True)
    elif data_type == "dateTime":
        if field_name in ["SinkCreatedOn", "SinkModifiedOn"]:
            # Handle custom timestamp format for SinkCreatedOn and SinkModifiedOn
            return StructField(field_name, StringType(), True)
        else:
            return StructField(field_name, TimestampType(), True)
    elif data_type == "dateTimeOffset":
        return StructField(field_name, TimestampType(), True)
    elif data_type == "decimal":
        return StructField(field_name, DecimalType(38, 18), True)
    elif data_type == "double":
        return StructField(field_name, DoubleType(), True)
    elif data_type == "boolean":
        return StructField(field_name, BooleanType(), True)
    else:
        return StructField(field_name, StringType(), True)
    

# Function to apply custom formatting and convert to TimestampType
def apply_custom_formatting(df):
    custom_format = "M/d/yyyy h:mm:ss a"
    if 'SinkCreatedOn' in df.columns:
        df = df.withColumn('SinkCreatedOn', to_timestamp(col('SinkCreatedOn'), custom_format))
    if 'SinkModifiedOn' in df.columns:
        df = df.withColumn('SinkModifiedOn', to_timestamp(col('SinkModifiedOn'), custom_format))
    return df


# Reads the model schema file from the given URL and returns a dictionary representing the table schema and partitions.
# Returns a dictionary representing the table schema and partitions, where the keys are table names and the values are
# the corresponding table schema/partitions.
def get_table_schema_dict(folder) -> {}:
    # Load the folder-specific model.json using the UC volume
    schema_file = f"{synapse_link_shortcut_path}/{folder}/model.json"
    with open(schema_file, "r", encoding="utf-8") as f:
        schema = json.load(f)

    # Build the schema dict
    table_schema_dict = {}

    # Only include tables that includes partitions
    # Tables with no listed partitions have not exported any files in the given folder
    for table in schema["entities"]:
        if "partitions" in table and table["partitions"]:

            table_name = None
            table_schema = StructType()
            for table_attribute in table["attributes"]:

                # New table name? Add the previous table schema to dict
                if table_name is not None and table_name != table["name"]:
                    schema_and_partitions_dict = {
                        "schema": table_schema,
                        "partitions": table["partitions"],
                    }
                    table_schema_dict[table_name] = schema_and_partitions_dict
                    table_schema = StructType()

                table_name = table["name"]
                table_schema.add(
                    get_struct_field(
                        table_attribute["name"], table_attribute["dataType"]
                    )
                )

            # Process the last table after the loop exits
            schema_and_partitions_dict = {
                "schema": table_schema,
                "partitions": table["partitions"],
            }

            table_schema_dict[table_name] = schema_and_partitions_dict

    return table_schema_dict


# We use the persisted last_merge_timestamp from the table to determine the starting point
def get_folders():
    folders = []
    last_processed_folder = get_last_processed_folder()

    # Loop through the PREV_FOLDERS in reverse, so we process the last folders first
    # Add folder if it has not previously been merged
    # The date format of the timestamp folders are lexicographically sortable, so we can
    # just compare our persisted folder name with the folder list to and add folders that are newer.
    for folder in reversed(PREV_FOLDERS):
        if last_processed_folder is None or folder > last_processed_folder:
            folders.append(folder)

    # Reverse the order of the result, so that folders are ordered chronologically
    folders.reverse()

    return folders

# COMMAND ----------

# Merge logic

def merge_incremental_table(table_name, folder, schema_and_partitions_dict):
    for partition in schema_and_partitions_dict["partitions"]:
        merge_incremental_csv_file(
            table_name, folder, partition["name"], schema_and_partitions_dict["schema"]
        )


# Function to check if a table exists in Unity Catalog
def table_exists(table_path):
    try:
        spark.table(table_path)
        return True
    except:
        return False
    

def merge_incremental_csv_file(table_name, folder, partition_name, schema):
    
    fabric_table_path = f"test_udap.base.{table_name}_incremental"
    partition_file = f"{folder}/{table_name}/{partition_name}.csv"
    #print(f'partition_file: {partition_file}')
    table_partition_file = f'{synapse_link_shortcut_path}/{partition_file}'
    #print(f'table_partition_file: {table_partition_file}')
    logging.info(f"Loading csv data from {partition_file}..")
    print(f"Loading csv data from {partition_file}..")

    csv_data_df = (
        spark.read.format("csv")
        .option("header", False)
        .option("multiline", True)
        .option("delimiter", ",")
        .option("quote", '"')
        .option("escape", "\\")
        .option("escape", '"')
        .schema(schema)
        .load(table_partition_file)
    )

    # We need to ensure only the latest record for a given id is present in source df or we will get ambiguity errors.
    # Define the window specification with partition by 'id' and order by 'IsDelete' and 'sysrowversion' descending.
    # sysrowversion is null for deletes, so we ensure that the deletion change takes priority
    windowSpec = Window.partitionBy("id").orderBy(
        desc("IsDelete"), desc("sysrowversion")
    )

    # Add a row number over the window specification
    df_with_row_number = csv_data_df.withColumn(
        "merge_row_number", row_number().over(windowSpec)
    )

    # Filter out only the rows with row_number = 1; these are the latest relevant records per id
    latest_records_df = df_with_row_number.filter(
        df_with_row_number.merge_row_number == 1
    ).drop("merge_row_number")

    handle_sink_datetime_format = apply_custom_formatting(latest_records_df)

    final_df = handle_sink_datetime_format.withColumn("partition_col", lit(partition_name))

    # Does the table exist? If not create it from our df.
    # If it exists, proceed with merge instead
    if not table_exists(fabric_table_path):
        final_df.write.mode("overwrite").format("delta").partitionBy("partition_col").saveAsTable(fabric_table_path)
        logging.info(f"Created table {fabric_table_path}")
        print(f"Created table {fabric_table_path}")
    else:
        # Merge:

        logging.info(f"Merging {partition_file} to '{fabric_table_path}'")
        print(f"Merging {partition_file} to '{fabric_table_path}'")
        destination_table = DeltaTable.forName(spark, fabric_table_path)

        # Only update records from sources that has a newer sysrowversion (thus skipping over previously processed records)
        # Important note: sysrowversion is not provided for deleted records, so deleted records should always update when matched.
        destination_table.alias("destination").merge(
            final_df.alias("source"), "source.id = destination.id"
        ).whenMatchedUpdateAll(
            condition="source.IsDelete = 1 OR source.sysrowversion > destination.sysrowversion"
        ).whenNotMatchedInsertAll().execute()

# COMMAND ----------

# Iterate new folders and merge
folders = get_folders()

with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
    for folder in folders:
        #logger.info(f"Processing folder {folder}..")
        print(f"Processing folder {folder}..")

        # Load the correct schema matching the change folder
        table_schema_dict = get_table_schema_dict(folder)
        table_names = list(table_schema_dict.keys())

        # Filter the tables in scope during testing
        filter_list = ('dirorganization','dirorganizationbase','dirpartytable','dirperson','salesline','salestable')
        #26min initial load - 4 threads on Developer Cluster UC
        #31min initial load - 16 threads on Developer Cluster UC
        #30min initial load - 8 threads on Developer Cluster UC

        #filter_list = ('dirperson') 
        table_names = [name for name in table_names if name in filter_list]

        # Submit merge command(s) to the executor
        future_to_table = {
            executor.submit(
                merge_incremental_table,
                table_name,
                folder,
                table_schema_dict[table_name],
            ): table_name
            for table_name in table_names
        }

        # Check the results of the futures
        try:
            # This will raise an exception if the callable raised one
            for future in concurrent.futures.as_completed(future_to_table):
                table_name = future_to_table[future]
                result = future.result()
        except Exception as e:
            #logger.critical(f"Error on {folder}/{table_name}: {e}", exc_info=True)
            print(f"Error on {folder}/{table_name}: {e}")
            raise e

        # All tables merged for the folder
        # Update last_processed_folder watermark
        save_last_processed_folder(folder)

#logger.info("Merge complete")
print("Merge complete")

# COMMAND ----------

dbutils.notebook.exit('successfully completed')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from test_udap.base.salestable_incremental

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from test_udap.base.dirperson_incremental

# COMMAND ----------

try:
    files = dbutils.fs.ls('/Volumes/test_udap/raw/synapse_link_test')
    for f in files:
        print(f.name)
except Exception as e:
    print(f"Error accessing root path: {e}")
