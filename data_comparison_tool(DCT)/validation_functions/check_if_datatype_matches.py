# Databricks notebook source
def check_if_datatype_matches(): 
  is_datatype_matching ='No'
  
  try:
    if (is_schema_matching == 'Yes'):
        source_columns = df_source.columns
        destination_columns = df_destination.columns

        column_mismatches = []

        for column in source_columns:
            if column in destination_columns:
                source_data_type = df_source.schema[column].dataType
                destination_data_type = df_destination.schema[column].dataType

                if source_data_type != destination_data_type:
                    column_mismatches.append(column)

        if len(column_mismatches) == 0:
            is_datatype_matching = 'Yes'
        else:
            for datatype_mismatching_column_names in column_mismatches:
                ic(datatype_mismatching_column_names)
    else:
        is_datatype_matching ="Couldnt compare data types due to mismatching schema."

  except Exception as error_message_for_datatype_matching:
    print("-----------ERROR--------------")
    ic(error_message_for_datatype_matching)  

  finally:
    ic(is_datatype_matching)
    return is_datatype_matching
