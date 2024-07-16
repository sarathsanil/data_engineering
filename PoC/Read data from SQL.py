# Databricks notebook source
# DBTITLE 1,Using Azure SQL
connectionstring = "jdbc:sqlserver://rules.database.windows.net:1433;database=Tucana_RSL_28_NOV_23;user=rulesreadonly@rules;password=rulesteam1!;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

connectionstring = "jdbc:sqlserver://20.33.59.5:1433;user=rules;password=analysisteam1!;database=test;"

# COMMAND ----------

# DBTITLE 1,Using SQL Server on Azure VM
connectionstring ="jdbc:sqlserver://20.33.59.5:1433;DatabaseName=Tucana_AWKEYFIX_03_Mar_2024;encrypt=true;trustServerCertificate=true;user=rulesreadonly;password=rulesteam1!;"

# COMMAND ----------

connectionstring ="jdbc:sqlserver://20.33.59.5:1433;DatabaseName=Tucana_AWKEYFIX_03_Mar_2024;user=rulesreadonly;password={YourPasswordHere};trustServerCertificate=true;"

# COMMAND ----------

df_sql = spark.read.jdbc(connectionstring, 'erp.BSEG')

# COMMAND ----------

df_sql.count()

# COMMAND ----------

remote_table = (spark.read
  .format("sqlserver")
  .option("host", "rules")
  #.option("port", "port") # optional, can use default port 1433 if omitted
  .option("user", "rules")
  .option("password", "analysisteam1!")
  .option("database", "test")
  .option("dbtable", "dbo.testtable") # (if schemaName not provided, default to "dbo")
  .load()
)

