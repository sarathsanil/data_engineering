from delta.tables import DeltaTable
df_delta = DeltaTable.forPath(spark,fullpath)
history_df = df_delta.history()
history_df.display()
spark.read.format(extension).option("versionAsOf",1).load(fullpath).display()
