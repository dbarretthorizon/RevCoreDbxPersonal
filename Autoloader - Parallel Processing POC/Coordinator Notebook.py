# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Useful Youtube breakdown of Autoloader from Databricks [here](https://www.youtube.com/watch?v=8a38Fv9cpd8&t=440s)
# MAGIC
# MAGIC The queues are visible on the storage account [here](https://portal.azure.com/#@HorizonDiscoveryGroup.onmicrosoft.com/resource/subscriptions/938153a9-59d4-48f1-9653-969020bac59d/resourceGroups/hd-datateam-synapse/providers/Microsoft.Storage/storageAccounts/hddatateamlaketest/eventgrid)

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to run a worker notebook
def run_notebook(table, key_columns, change_columns):
    result = dbutils.notebook.run("/Workspace/Users/daniel.barrett@horizondiscovery.com/Autoloader/Parallel processing Example/Worker Notebook", 0, {
        "table": table,
        "key_columns": key_columns,
        "change_columns": change_columns
    })
    return table, result

# COMMAND ----------

# Dictionary containing table configurations
table_key_columns = {
    "raw_to_base_autoloader_test_2": {
        "key_columns": "key1",
        "change_columns": "attribute1,measure1"
    },
    "raw_to_base_autoloader_test_3": {
        "key_columns": "key1,attribute1",
        "change_columns": "measure1"
    },
    "raw_to_base_autoloader_test_4": {
        "key_columns": "key1,measure1",
        "change_columns": "attribute1"
    }
}

# Limit the number of concurrent notebooks
max_concurrent_notebooks = 3

# Create a thread pool
with ThreadPoolExecutor(max_workers=max_concurrent_notebooks) as executor:
    future_to_table = {
        executor.submit(run_notebook, table, attributes["key_columns"], attributes["change_columns"]): table
        for table, attributes in table_key_columns.items()
    }
    
    for future in as_completed(future_to_table):
        table = future_to_table[future]
        try:
            result = future.result()
            print(f"Autoloader process on Table {result[0]} completed with result: {result[1]}")
        except Exception as e:
            print(f"Autoloader process on Table {table} generated an exception: {e}")

# COMMAND ----------

dbutils.notebook.exit("exit")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_udap.base.sap_raw_to_base_autoloader_test_2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_udap.base.sap_raw_to_base_autoloader_test_3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_udap.base.sap_raw_to_base_autoloader_test_4

# COMMAND ----------

table_list = ('sap_raw_to_base_autoloader_test_2','sap_raw_to_base_autoloader_test_3','sap_raw_to_base_autoloader_test_4')
for table in table_list:
    try:
        spark.sql(f"drop table test_udap.base.{table}")
        print(f"dropped {table}")
    except:
        print(f"skipped {table}")
