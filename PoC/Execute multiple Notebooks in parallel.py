# Databricks notebook source
# DBTITLE 1,Creating a Notebook Class for passing the path
class NotebookData:
  def __init__(self, path, timeout, parameters=None, retry=0):
     self.path = path
     self.timeout = timeout
     self.parameters = parameters
     self.retry = retry

# COMMAND ----------

# DBTITLE 1,Creating Run Notebook List for iterating
tables = []
for name in dbutils.fs.ls(source_tablepath):
    tables.append(name.name.rstrip("/"))

# tables = ["dwh_L1_TD_Account_Process.delta","dwh_L1_TD_Account_Process.delta"]
list_run_notebooks = []
for table_name in tables:
    list_run_notebooks.append(
        NotebookData(
            "./generic_Data_Validation_scripts",
            0,
            {
                "1_storageaccount": storageaccount,
                "2_containername": containername,
                "3_source_subdirectory": source_subdirectory,
                "4_destination_subdirectory": destination_subdirectory,
                "5_folder_types": folder_types,
                "6_tablename": table_name,
                "7_use_database": use_database,
            },
        )
    )

len(list_run_notebooks)

# COMMAND ----------

# DBTITLE 1,Creating Submit Notebook Method
def submitNotebook(notebook):
   print("Running notebook %s" % notebook.path + "\n")
   try:
     if (notebook.parameters):
       return dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
     else:
       return dbutils.notebook.run(notebook.path, notebook.timeout)
   except Exception:
     if notebook.retry < 1:
       raise
     print("Retrying notebook %s" % notebook.path + "\n")
     notebook.retry = notebook.retry - 1
     submitNotebook(notebook)

# COMMAND ----------

# DBTITLE 1,Creating a parallel execution Method
def parallelNotebooks(notebooks, numInParallel):
   with ThreadPoolExecutor(max_workers=numInParallel) as ec:
     return [ec.submit(submitNotebook, notebook) for notebook in notebooks]

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

res = parallelNotebooks(list_run_notebooks, len(list_run_notebooks))
