# Databricks notebook source
def create_database(passed_db_name):
  try:
    if(passed_db_name==''):
      now = datetime.now()
      datetime_str = now.strftime('%d%b%Y_%H_%p')
      catalog_database_name = 'data_comparison_' + datetime_str
    else:      
      catalog_database_name = passed_db_name

    create_database = f"CREATE DATABASE IF NOT EXISTS {catalog_database_name}"

    use_database = f"USE {catalog_database_name}"
    spark.sql(create_database)
    spark.sql(use_database)
    catalog_table_name = f"tbl_{table_name_prefix}_{folder_type}".replace("-", "_").replace("/", "_")

    ic(catalog_database_name)
    ic(catalog_table_name)

  except Exception as error_message_while_creating_database:
    print("-----------ERROR:-------------")
    ic(error_message_while_creating_database)  
  finally:
    return(catalog_database_name)
