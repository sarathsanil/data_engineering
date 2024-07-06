# Databricks notebook source
def check_if_schema_matches(): 
  is_schema_matching ='No'
  try:
    if (source_table_exists) and (destination_table_exists):
      if((set(df_source.columns) - set(df_destination.columns)) or (set(df_destination.columns) - set(df_source.columns))): 
        print("Source and Destination schema are not matching.")      
        if(set(df_source.columns) - set(df_destination.columns)):
          print("Source has below extra columns.")
          print(set(df_source.columns) - set(df_destination.columns))
        
        if(set(df_destination.columns) - set(df_source.columns)):
          print("Destination has below extra columns.")
          print(set(df_destination.columns) - set(df_source.columns))  
      else:
        is_schema_matching ='Yes'

    else:
        is_schema_matching ='Couldnt compare schema due to Table not found'
    
    ic(is_schema_matching) 

  except Exception as error_message_for_schema_matching:
    print("-----------ERROR--------------")
    ic(error_message_for_schema_matching)  

  finally:
    return is_schema_matching
