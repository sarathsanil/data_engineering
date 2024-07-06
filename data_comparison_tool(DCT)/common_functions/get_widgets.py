# Databricks notebook source
def get_widgets(notebook_name = "bulk"):
  
  storageaccount = dbutils.widgets.get("1_storageaccount") 
  containername = dbutils.widgets.get("2_containername")
  source_subdirectory = dbutils.widgets.get("3_source_subdirectory")
  destination_subdirectory = dbutils.widgets.get("4_destination_subdirectory") 
  folder_type = dbutils.widgets.get('5_folder_type')
  source_raw_sub_folder = dbutils.widgets.get('5_source_rawFiles_sub_folder')
  destination_raw_sub_folder = dbutils.widgets.get('5_destination_rawFiles_sub_folder')
  table_name_prefix = dbutils.widgets.get('7_prefix_text_in_tablename')
  database_name = dbutils.widgets.get('8_database_name')

  if(notebook_name == "single"):    
    tablename = dbutils.widgets.get("6_tablename")
    ic(storageaccount, containername, source_subdirectory, destination_subdirectory, folder_type, source_raw_sub_folder, destination_raw_sub_folder,tablename, table_name_prefix, database_name)
    return storageaccount, containername, source_subdirectory, destination_subdirectory, folder_type, source_raw_sub_folder, destination_raw_sub_folder,tablename, table_name_prefix, database_name   
  else:
    ic(storageaccount, containername, source_subdirectory, destination_subdirectory, folder_type, source_raw_sub_folder, destination_raw_sub_folder, table_name_prefix, database_name)
    return storageaccount, containername, source_subdirectory, destination_subdirectory, folder_type, source_raw_sub_folder, destination_raw_sub_folder, table_name_prefix, database_name 
