# Databricks notebook source
def get_full_paths(is_file_name_required=True):
  try:
    source_fullpath = ''
    destination_fullpath =''
    abfss_path = f'abfss://{containername}@{storageaccount}.dfs.core.windows.net'

    if(storageaccount == 'audeusdevsalcw02'):
      spark.conf.set(
          f"fs.azure.account.key.{storageaccount}.dfs.core.windows.net",
          "GIVE the key here",
      )
    elif(storageaccount == 'audeusdlsdevkcwgaap01'):
      spark.conf.set(
          f"fs.azure.account.key.{storageaccount}.dfs.core.windows.net",
          "GIVE the key here",
      )      


    if folder_type == 'rawFiles':
        source_fullpath = f'{abfss_path}/{source_subdirectory}/{folder_type}/{source_raw_sub_folder}'
        destination_fullpath = f'{abfss_path}/{destination_subdirectory}/{folder_type}/{destination_raw_sub_folder}'
    else:
        source_fullpath= f'{abfss_path}/{source_subdirectory}/{folder_type}'
        destination_fullpath = f'{abfss_path}/{destination_subdirectory}/{folder_type}'

    if (is_file_name_required):
      source_fullpath= f'{source_fullpath}/{tablename}'
      destination_fullpath =f'{destination_fullpath}/{tablename}'

    # else:
    #   source_fullpath = f'/mnt/{storageaccount}-{containername}/{source_tablepath}'
    #   destination_fullpath = f'/mnt/{storageaccount}-{containername}/{destination_tablepath}'

  except Exception as error_message_for_finding_table_paths:
    print("-----------ERROR:-------------")
    ic(error_message_for_finding_table_paths)  
  finally:
    ic(source_fullpath,destination_fullpath)
    return source_fullpath, destination_fullpath
