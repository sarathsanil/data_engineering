# Databricks notebook source
# DBTITLE 1,EXISTING CODE with LIST
for analyticNames in gl_lstOfScopedAnalytics:
  [listoffiles.append([h.analyticName, h.fileName, h.ColumnName, h.category, h.hierarchyID]) \
  for h in gl_metadataDictionary['dic_ddic_deleteEngagementTables'].select(col("fileName"), \
              col("analyticName"), col("ColumnName"), col("category"), \
              col("hierarchyID")). \
              filter((col("hierarchyID") > 0) & (col('analyticName')==analyticNames)). \
              orderBy(col("hierarchyID")).rdd.collect()]

executionStartTime = datetime.now()

def execute_delete(sublist):
  DMOrchestrationHelper.gen_deleteEngagement(sublist[0], sublist[1], sublist[2], sublist[3])
with ThreadPoolExecutor(max_workers = MAXPARALLELISM.DEFAULT.value) as executor:
  executor.map(execute_delete, listoffiles)

# COMMAND ----------

# DBTITLE 1,NEW CODE WITH DATAFRAME LAZY EVALUATION
def delete_engagement_based_on_analyticName(iterator):
    for row in iterator:
        DMOrchestrationHelper.gen_deleteEngagement(row.fileName, row.analyticName, row.ColumnName, row.category, row.hierarchyID)

engagementlist_partitioned_data = gl_metadataDictionary['dic_ddic_deleteEngagementTables'] \
    .select("fileName", "analyticName", "ColumnName", "category", "hierarchyID") \
    .filter(col("hierarchyID") > 0) \
    .repartition(col("analyticName"))

# Process each partition
for analyticName in gl_lstOfScopedAnalytics:
    df = engagementlist_partitioned_data.filter(col('analyticName') == analyticName)

    df.foreachPartition(delete_engagement_based_on_analyticName)
