# Databricks notebook source
def check_if_count_matches():
  source_table_count =''
  destination_table_count =''
  is_count_matching =''
  try:
    if (source_table_exists):
        source_table_count = df_source.count()
    else:    
        is_count_matching = 'Couldnt continue due to Source Table not found' 
        source_table_count = 'Source Table not found' 

    if (destination_table_exists):
        destination_table_count = df_destination.count()
    else:
        is_count_matching = 'Couldnt continue due to Destination Table not found'
        destination_table_count = 'Destination Table not found'

    if (source_table_exists) and (destination_table_exists):
        if source_table_count == destination_table_count:
            is_count_matching = 'Yes'    
        else:            
            is_count_matching = 'No'    

    ic(tablename)

  except Exception as error_message_for_count_matching:
    print("-----------ERROR--------------")
    ic(error_message_for_count_matching)  

  finally:
    ic(source_table_count,destination_table_count,is_count_matching)
    return source_table_count,destination_table_count,is_count_matching
