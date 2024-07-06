# Databricks notebook source
pip install icecream

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from icecream import *
from datetime import datetime

# COMMAND ----------

# MAGIC %run ./compile/single_comparison_functions

# COMMAND ----------

# DBTITLE 1,Creating &  Getting values from widgets
create_widgets('single')
storageaccount, containername, source_subdirectory, destination_subdirectory, folder_type, source_raw_sub_folder, destination_raw_sub_folder, tablename, table_name_prefix, database_name = get_widgets('single')

# COMMAND ----------

# DBTITLE 1,Setting up variables with values
use_database = f'USE {database_name}'
spark.sql(use_database)
source_fullpath, destination_fullpath = get_full_paths(True)

# COMMAND ----------

# DBTITLE 1,Checking if the table EXISTS or not
# col1_table_name =  tablename # USED ONLY In the final insert
source_table_exists,destination_table_exists = check_if_table_exists()
df_source, df_destination = create_dataframes()

# COMMAND ----------

# DBTITLE 1,Reading the File to dataframes
df_source, df_destination = create_dataframes()

# COMMAND ----------

# DBTITLE 1,Checking if COUNT matches or not
source_table_count,destination_table_count,is_count_matching = check_if_count_matches()

# COMMAND ----------

# DBTITLE 1,Checking if SCHEMA matches or not
is_schema_matching = check_if_schema_matches()

# COMMAND ----------

# DBTITLE 1,checking if DATATYPE matches or not
is_datatype_matching = check_if_datatype_matches()

# COMMAND ----------

# DBTITLE 1,Checking if DATA matches or not
is_data_matching = check_if_data_matches()

# COMMAND ----------

# DBTITLE 1,get the List of columns to be excluded from data comparison
columns_to_exclude_from_comparison = get_columns_to_exclude_from_comparison()

# COMMAND ----------

# DBTITLE 1,Displays the mismatch columns & count of records in each
data_matching_columns,data_mismatching_columns = get_columnlevel_datacomparison()

# COMMAND ----------

write_to_database()
