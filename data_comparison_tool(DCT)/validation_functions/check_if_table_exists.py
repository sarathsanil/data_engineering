# Databricks notebook source
def check_if_table_exists():
  try:
      dbutils.fs.ls(source_fullpath)
      source_table_exists = True
  except Exception as error_message_for_source_table:
    print("------ERROR------SOURCE FILE NOT FOUND------")       
    source_table_exists = False
    df_source = None

  try:
      dbutils.fs.ls(destination_fullpath)
      destination_table_exists = True
  except Exception as error_message_for_destination_table:
    print("------ERROR------DESTINATION FILE NOT FOUND------")
    destination_table_exists = False
    df_destination = None

  finally:
    # ic(source_table_exists,destination_table_exists)
    return source_table_exists, destination_table_exists
