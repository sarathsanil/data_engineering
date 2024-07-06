# Databricks notebook source
pip install icecream

# COMMAND ----------

from icecream import ic
from datetime import datetime

# COMMAND ----------

# MAGIC %run ./compile/bulk_comparison_functions

# COMMAND ----------

create_widgets('bulk')
storageaccount, containername, source_subdirectory, destination_subdirectory, folder_type, source_raw_sub_folder, destination_raw_sub_folder, table_name_prefix, database_name = get_widgets('bulk')

# COMMAND ----------

source_fullpath, destination_fullpath = get_full_paths(False)

tables = []
for name in  dbutils.fs.ls(source_fullpath):
  if '.' in name.name:  # Skip folders
    tables.append(name.name.rstrip('/'))

for name in  dbutils.fs.ls(destination_fullpath):
  if('.' in name.name):
    if(name.name.rstrip('/') not in tables):
      tables.append(name.name.rstrip('/'))

print(f'{len(tables)} objects will be iterated')

# COMMAND ----------

database_name = create_database(database_name)

# COMMAND ----------

for table_name in tables:
    dbutils.notebook.run(
        "./single_comparison",
        0,
        {
            "1_storageaccount": storageaccount,
            "2_containername": containername,
            "3_source_subdirectory": source_subdirectory,
            "4_destination_subdirectory": destination_subdirectory,
            "5_folder_type": folder_type,
            "5_source_rawFiles_sub_folder": source_raw_sub_folder,
            "5_destination_rawFiles_sub_folder": destination_raw_sub_folder,
            "6_tablename": table_name,
            "7_prefix_text_in_tablename": table_name_prefix,
            "8_database_name": database_name
        },
    )
    print(f'Executed notebook  for {table_name}')
