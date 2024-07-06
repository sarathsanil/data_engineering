# Databricks notebook source
def get_columnlevel_datacomparison():
  data_matching_columns =""
  data_mismatching_columns =""
  lst_matching_columns = []
  lst_mismatching_columns =[]

  try:
    if(is_schema_matching == 'Yes' and is_count_matching == 'Yes'):
        
        column_names =df_destination.columns        
        included_columns_for_comparison = [col for col in column_names if col not in columns_to_exclude_from_comparison]
        
        df_destination.cache()
        df_source.cache()
        # Iterate through each column
        for column_name in included_columns_for_comparison:
            # Create DataFrames with a single column
            df_destination_few = df_destination.select(column_name).alias("dest")
            df_source_few = df_source.select(column_name).alias("src")

            # Compare DataFrames and count mismatches
            mismatch_values_destination = df_destination_few.exceptAll(df_source_few).count()
            mismatch_values_source = df_source_few.exceptAll(df_destination_few).count()

            if mismatch_values_destination == 0 and mismatch_values_source == 0 :
                lst_matching_columns.append(column_name)
            else:
                lst_mismatching_columns.append(column_name)        

        data_matching_columns = ','.join(lst_matching_columns)
        data_mismatching_columns = ','.join(lst_mismatching_columns)

        df_destination.unpersist()    
        df_source.unpersist()

    else: 
        data_matching_columns = 'Didnt compare due to Schema or Count mismatch'
        data_mismatching_columns = 'Didnt compare due to Schema or Count mismatch'


  except Exception as error_message_for_columnlevel_datacomparison:
    print("-----------ERROR--------------")
    ic(error_message_for_columnlevel_datacomparison)  

  finally:
    # ic(data_matching_columns,data_mismatching_columns)
    print(f"data_matching_columns={data_matching_columns}")
    print(f"data_mismatching_columns={data_mismatching_columns}")    
    return data_matching_columns,data_mismatching_columns
