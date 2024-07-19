import pathlib
from pathlib import Path
from pyspark.sql.functions import col, lit


def convert_files_to_dataframe(rootpath="", foldername=""):
    try:
        fullpath = rootpath + foldername
        for files in dbutils.fs.ls(fullpath):
            filename = pathlib.Path(files[0]).stem
            extension = pathlib.Path(files[0]).suffix.lower()

            if (extension) == ".delta":
                df_in_each_loop = spark.read.format("delta").load(files[0])

            elif (extension) == ".parquet":
                df_in_each_loop = spark.read.format("parquet").load(files[0])

            else:
                continue

            dynamic_df_name = f"{filename}"
            globals()[dynamic_df_name] = df_in_each_loop
            df_in_each_loop.createOrReplaceGlobalTempView(dynamic_df_name)

    except Exception as err:
        raise