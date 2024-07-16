# Databricks notebook source
def add(x,y):
  
  print (x+y)
  print(ic(x,y))
  print(x*y) 


# COMMAND ----------

pip install icecream

# COMMAND ----------

from icecream import ic

# COMMAND ----------

print(add(2,3))
