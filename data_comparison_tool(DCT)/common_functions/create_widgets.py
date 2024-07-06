# Databricks notebook source
def create_widgets(notebook_name="bulk"):
  dbutils.widgets.text("1_storageaccount", "") 
  dbutils.widgets.text("2_containername", "") 
  dbutils.widgets.text("3_source_subdirectory", "")
  dbutils.widgets.text("4_destination_subdirectory", "")
  dbutils.widgets.dropdown("5_folder_type","cdm-l1",["cdm-l1","cdm-l2","staging/l1-stg","staging/l0-tmp","staging/l2-stg","rawFiles"])
  dbutils.widgets.text("5_source_rawFiles_sub_folder","")
  dbutils.widgets.text("5_destination_rawFiles_sub_folder","")
  dbutils.widgets.text("7_prefix_text_in_tablename", "")
  dbutils.widgets.text("8_database_name", "")

  if(notebook_name == "single"):
    tablename = dbutils.widgets.text("6_tablename", "")
