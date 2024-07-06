# Databricks notebook source
def check_if_data_matches(): 
  is_data_matching ='No'
  
  try:
    if (is_schema_matching == 'Yes' and is_datatype_matching == 'Yes' and is_count_matching == 'Yes'):
      df_difference = df_source.subtract(df_destination)
      count_of_rows_with_difference = df_difference.count()
      ic(count_of_rows_with_difference)
      is_data_matching='No' if count_of_rows_with_difference > 0 else 'Yes'
    else:
        is_data_matching ='Didnt compare due to Schema or Count mismatch'

  except Exception as error_message_for_data_matching:
    print("-----------ERROR--------------")
    ic(error_message_for_data_matching)  

  finally:
    ic(is_data_matching)
    return is_data_matching
