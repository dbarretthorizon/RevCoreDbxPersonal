# Databricks notebook source
from pyspark.sql.functions import input_file_name, sha2, concat_ws, col, lit
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from datetime import datetime

current_time = datetime.now().isoformat()

def add_metadata_columns(df: DataFrame, lKeyColumns: list, lChangeColumns: list) -> DataFrame:
    """
    Adds metadata columns for key hash and change hash to a DataFrame. The key hash is computed 
    as a SHA-256 hash of the specified key columns, and the change hash is computed as a SHA-256 hash 
    of the change columns. It also adds a 'MetadataDateLastUpdated' column set to a fixed date-time value.
    Parameters:
    df (dataframe): The DataFrame to which metadata columns will be added.
    lKeyColumns (list): A list of column names used to compute the 'MetadataKeyHash'.
    lChangeColumns (list): A list of column names used to compute the 'MetadataChangeHash'.
    Returns:
    DataFrame: The modified DataFrame with added metadata columns.
    """
    keyHashExpr = sha2(concat_ws('|', *[col(c).cast("String") for c in lKeyColumns]), 256).alias('MetadataKeyHash')
    changeHashExpr = sha2(concat_ws('|', *[col(c).cast("String") for c in lChangeColumns]), 256).alias('MetadataChangeHash')
    df = df.select(keyHashExpr, changeHashExpr, *df.columns)
    return df

# COMMAND ----------

# Worker Notebook

connectionString = dbutils.secrets.get(scope="key-vault-secret-AZWEUKVDATATEAMGENTEST", key="DataTeamLakeConnStr")
appRegistrationSecret = dbutils.secrets.get(scope="key-vault-secret-AZWEUKVDATATEAMGENTEST", key="DatabricksAppRegistrationSecret")

# Build CloudFiles config
cloudfile = {
    "cloudFiles.format":"csv",
    "cloudFiles.useNotifications":"true",
    "cloudFiles.connectionString":connectionString,
    "cloudFiles.resourceGroup":"hd-datateam-synapse",
    "cloudFiles.subscriptionId":"938153a9-59d4-48f1-9653-969020bac59d",
    "cloudFiles.tenantId":"6252adb0-2e06-467f-b35d-dbd8905869ba",
    "cloudFiles.clientId":"bf52da41-35fa-46e3-b285-c20001895ff8",
    "cloudFiles.clientSecret":appRegistrationSecret,
    "header":"true",
    "cloudFiles.schemaEvolutionMode":"rescue", #rescue, addNewColumns, failOnNewColumns, none
    #"cloudFiles.rescuedDataColumn":"_rescued_data",
    "cloudFiles.inferColumnTypes":"True",
    #"cloudFiles.schemaLocation":"/Volumes/test_udap/raw/default/autoloader_test/inferred_schema",
    "cloudFiles.schemaHints":"key1 string" #override inference for specified column names
}


# Retrieve parameters
dbutils.widgets.text("table", "")
dbutils.widgets.text("key_columns", "")
dbutils.widgets.text("change_columns", "")

table = dbutils.widgets.get("table")
key_columns = dbutils.widgets.get("key_columns")
change_columns = dbutils.widgets.get("change_columns")

def upsertToDelta(microBatchOutputDf, batchId, uc_table_name, key_columns):
    """
    Method to merge the data into target table
    """
    try:
        deltadf = DeltaTable.forName(spark, uc_table_name)
    except:
        print(f"Table {uc_table_name} does not exist. Creating table.")
        microBatchOutputDf = microBatchOutputDf.withColumn("MetadataDateCreated", lit(current_time).cast("timestamp"))
        microBatchOutputDf = microBatchOutputDf.withColumn("MetadataDateUpdated", lit(current_time).cast("timestamp"))
        microBatchOutputDf.write.format("delta").saveAsTable(uc_table_name)
        deltadf = DeltaTable.forName(spark, uc_table_name)

    # Perform the upsert operation
    merge_condition = "s.MetadataKeyHash = t.MetadataKeyHash"
    update_condition = "s.MetadataChangeHash <> t.MetadataChangeHash"
    
    update_set = {f"t.{col}": f"s.{col}" for col in microBatchOutputDf.columns}
    update_set["t.MetadataDateUpdated"] = lit(current_time).cast("timestamp")
    
    insert_set = {col: f"s.{col}" for col in microBatchOutputDf.columns}
    insert_set["MetadataDateCreated"] = lit(current_time).cast("timestamp")
    insert_set["MetadataDateUpdated"] = lit(current_time).cast("timestamp")

    (deltadf.alias("t")
     .merge(
         microBatchOutputDf.alias("s"),
         merge_condition)
     .whenMatchedUpdate(
         set=update_set,
         condition=update_condition)
     .whenNotMatchedInsert(
         values=insert_set)
     .execute()
    )

def has_files(directory):
    """
    Check if there are files to process
    """
    files = dbutils.fs.ls(directory)
    return any(file.name.endswith(".csv") for file in files if not file.name.startswith("_"))

def processAutoloaderTable(table, key_columns, change_columns):
    """
    Process SAP table via autoloader
    """
    filePath = f"/Volumes/test_udap/raw/default/autoloader_test/{table}"
    
    # Convert strings to lists
    key_columns = key_columns.split(',')
    change_columns = change_columns.split(',')

    if has_files(filePath):
        # Set up the readStream
        readStreamDf = (spark
              .readStream
              .format("cloudFiles")
              .options(**cloudfile)
              .option("cloudFiles.schemaLocation", f"/Volumes/test_udap/raw/default/autoloader_test/{table}/inferred_schema")
              #.schema(schema)  # Remove this line if schema inference is preferred
              .load(filePath).filter("input_file_name() LIKE '%.csv'")  # Needed to avoid trying to read from the _checkpoint file in the same directory!
              .withColumn("input_file_path", input_file_name())
             )

        # Add default metadata columns
        readStreamDf = add_metadata_columns(readStreamDf, key_columns, change_columns)

        # Define the wrapper function to pass additional parameters
        def batch_function(df, batchId):
            upsertToDelta(df, batchId, f"test_udap.base.sap_{table}", key_columns)

        # Set up the writeStream
        writeStreamDf = (readStreamDf.writeStream
                .outputMode("append")
                .foreachBatch(batch_function)
                .queryName(table)
                #.trigger(once=True)
                #.trigger(processingTime="1 minute")
                .option("checkpointLocation", f"/Volumes/test_udap/raw/default/autoloader_test/{table}/_checkpoint/")
                .start())
        
        writeStreamDf.awaitTermination()
    else:
        print(f"No files found in {filePath}. Skipping write stream.")

# Process the table
processAutoloaderTable(table, key_columns, change_columns)

# Return success message
dbutils.notebook.exit("Success")
