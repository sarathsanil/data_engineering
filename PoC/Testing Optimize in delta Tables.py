# Databricks notebook source
df = spark.read.format("delta").load("/mnt/audeusdlsdevkcwgaap01-rulesdevteam/E2E/Tucana/KCaPaaS_2023.2/2024/Mar/08/h-28FEBALL-TUCA-NA34-8073-2B46AWKEYFIX/cdm-l1/ptp_L1_TD_PurchaseHistory.delta")

# COMMAND ----------

from delta.tables import *
pathToTable ="/mnt/audeusdlsdevkcwgaap01-rulesdevteam/E2E/Tucana/KCaPaaS_2023.2/2024/Mar/07/h-28FEBALL-TUCA-NA34-8073-2B46AWKEYFIX/cdm-l1/ptp_L1_TD_PurchaseHistory.delta"
optimized_df = DeltaTable.forPath(spark, pathToTable)

# COMMAND ----------

#optimized_df.optimize().executeZOrderBy(eventType)

optimized_df.optimize().executeCompaction()

# COMMAND ----------

optimized_df.optimize().executeZOrderBy("purchaseHistoryPostingDate")

# COMMAND ----------

file_path = '/mnt/audeusdlsdevkcwgaap01-rulesdevteam/E2E/Tucana/KCaPaaS_2023.2/2024/Mar/07/h-28FEBALL-TUCA-NA34-8073-2B46AWKEYFIX/cdm-l1/ptp_L1_TD_PurchaseHistory.delta/part-00000-61e733ba-1e75-491d-8b4f-4764255f37cc-c000.snappy.parquet'

file_info = dbutils.fs.ls(file_path)

file_size = ((file_info[0].size)/1024)/1024

display(file_size)

# COMMAND ----------

directory_path = '/mnt/audeusdlsdevkcwgaap01-rulesdevteam/E2E/Tucana/KCaPaaS_2023.2/2024/Mar/07/h-28FEBALL-TUCA-NA34-8073-2B46AWKEYFIX/cdm-l1/'

file_paths = [file_info.path for file_info in dbutils.fs.ls(directory_path) if file_info.path.endswith('.delta')]

if len(file_paths) > 0:
    for file_path in file_paths:
        file_info = dbutils.fs.ls(file_path)
        file_size = file_info[0].size
        display(file_path + ': ' + str(file_size) + ' bytes')
else:
    display("No Delta files found in the directory.")
