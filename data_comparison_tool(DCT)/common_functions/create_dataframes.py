# Databricks notebook source
def create_dataframes():
  df_source = None
  df_destination = None
  try:
    extension = tablename.split('.')[-1]
  
    if (source_table_exists):
      df_source = spark.read.format(extension).load(source_fullpath)
      df_source = df_source.select([trim(col(c)).alias(c) for c in df_source.columns])
    else:
      print('Source table does not exist')

    if (destination_table_exists):
      df_destination = spark.read.format(extension).load(destination_fullpath)
      df_destination = df_destination.select([trim(col(c)).alias(c) for c in df_destination.columns])
    else:
      print('Destination table does not exist')
      
  except Exception as error_message_while_creating_dataframes:
    print("-----------ERROR:-------------")
    ic(error_message_while_creating_dataframes)  
  finally:    
    return df_source, df_destination
