# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Collect Set & List
data = [(1, "Sarath", 50), (1, "Sarath", 60), (1, "Sarath", 50)]

schema = ["id", "name", "marks"]

df = spark.createDataFrame(data, schema)

from pyspark.sql.functions import collect_list, col, collect_set

df2 = (
    df.groupBy(col("id"), col("name"))
    .agg(collect_list(col("marks")).alias("collated"))
    .select(col("id"), col("name"), col("collated"))
)

df2.display()

# COMMAND ----------

# DBTITLE 1,Adding Row_number
# Import necessary functions
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

WindowSpec = Window.partitionBy(col('marks')).orderBy(col('id'))
df_window = df.withColumn('Sno',row_number().over(WindowSpec))

df_window.display()

# COMMAND ----------

# DBTITLE 1,Pick only the top mark for each person
data = [(1, "Sarath", 50), (1, "Sarath", 60), (1, "Sarath", 50),(1, "Silpa", 70),(1, "Silpa", 90)]

schema = ["id", "name", "marks"]

df = spark.createDataFrame(data, schema)

#1
df_cte = df.withColumn('Sno',row_number().over(Window.partitionBy(col('name')).orderBy(col('marks'))))
df_cte.filter(col('Sno')==1).display()


#2
from pyspark.sql.functions import collect_list, col, collect_set, max
df_cte = df.groupBy(col('id'),col('name')).agg(max(col('marks')))
df_cte.display()

# COMMAND ----------

# DBTITLE 1,Find people having salary than average mark in each department
data = [( "Sarath",'DSA', 50), ( "Amar","DSA", 60), ( "Ammu",'Medical', 80),( "Silpa",'Medical', 70),( "Sanjan",'Medical', 90)]

schema = ["name",'dept', "marks"]

df = spark.createDataFrame(data, schema)

#df.display()

df.write.saveAsTable('info')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED info

# COMMAND ----------

# DBTITLE 1,Merge Statement

data = [( "Sarath",'DSA', 50), ( "Amar","DSA", 60), ( "Ammu",'Medical', 80),( "Silpa",'Medical', 70),( "Sanjan",'Medical', 90)]

schema = ["name",'dept', "marks"]

df = spark.createDataFrame(data, schema)

#df.display()

df.write.saveAsTable('info')
##########################################
from delta.tables import *

df_delta = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/info")

data = [
    ("Sarath", "DSA", 90),
    ("Amar", "DSA", 60),
    ("Ammu", "Medical", 80),
    ("Silpa", "Medical", 70),
    ("Sanjan", "Medical", 90),
]

schema = ["name", "dept", "marks"]

df_upd = spark.createDataFrame(data, schema)

# df_upd.display()
# df.display()

df_delta.alias("d").merge(
    df_upd.alias("s"), col("s.name") == col("d.name")
).whenMatchedUpdate(set={"d.marks": "s.marks"}).execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select *,avg(marks) over(partition by dept) as rwno from info
# MAGIC
# MAGIC -- ;with cte as (
# MAGIC -- select *,row_number() over(partition by dept order by marks) as [rwno],avg(marks) from info
# MAGIC -- )
# MAGIC -- select * from cte 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, dept, marks
# MAGIC FROM (
# MAGIC     SELECT name, dept, marks, 
# MAGIC            AVG(marks) OVER (PARTITION BY dept) AS avg_mark
# MAGIC     FROM info
# MAGIC ) subquery
# MAGIC WHERE marks > avg_mark;
# MAGIC
