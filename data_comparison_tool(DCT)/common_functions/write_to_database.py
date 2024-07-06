# Databricks notebook source
def write_to_database():
  output =[]
  
  try:
    schema = StructType([
        StructField("TableName", StringType(), nullable=False),
        StructField("CountMatch", StringType(), nullable=True),
        StructField("SourceCount", StringType(), nullable=True),
        StructField("DestCount", StringType(), nullable=True),    
        StructField("SchemaMatch", StringType(), nullable=True),
        StructField("DataTypeMatch", StringType(), nullable=True),
        StructField("DataMatch", StringType(), nullable=True),
        StructField("DataMatchingColumns", StringType(), nullable=True),
        StructField("DataMismatchingColumns", StringType(), nullable=True),
        StructField("ExcludedColumns", StringType(), nullable=True),              
        StructField("BatchRunTime", StringType(), nullable=True)
    ])

    time  = datetime.now()
    batch_run_time = time.strftime('%d%b%Y %H:%M:%S %p')

    columns_to_exclude_str = ','.join(columns_to_exclude_from_comparison)
    
    output.append((tablename,is_count_matching,source_table_count,destination_table_count,is_schema_matching,is_datatype_matching,is_data_matching,data_matching_columns,data_mismatching_columns,columns_to_exclude_str,batch_run_time))

    output_df = spark.createDataFrame(output, schema)
    table_name = f"tbl_{table_name_prefix}_{folder_type}".replace("-", "_").replace("/", "_")
    output_df.write.mode("append").option('mergeSchema', 'true').saveAsTable(table_name)    
    
    print(f'select * from {use_database[4:]}.{table_name}')
    output_df.display()

  except Exception as error_message_while_writing_to_database:
    print("-----------ERROR:-------------")
    ic(error_message_while_writing_to_database)      
