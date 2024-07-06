# Databricks notebook source
def get_columns_to_exclude_from_comparison():
    tbl_exclusionColumnAttributeList = "default.tbl_exclusionColumnAttributeList"
    columns_to_exclude_from_comparison = []
    try:
        df_exclusionColumnAttributeList = (
            spark.table(f"{tbl_exclusionColumnAttributeList}")
            .filter((col("isToBeExcluded") == 1))
            .filter(col("tableName") == tablename)
        )

        if df_exclusionColumnAttributeList.first():
            columns_to_exclude_from_comparison = (
                df_exclusionColumnAttributeList.groupBy(col("tableName"))
                .agg(
                    col("tableName"),
                    collect_list(col("columnName")).alias("listOfColumnName"),
                )
                .select(col("tableName"), col("listOfColumnName"))
                .collect()[0]["listOfColumnName"]
            )

    except Exception as error_message_for_get_columns_to_ignore:
        print("-----------ERROR--------------")
        ic(error_message_for_get_columns_to_ignore)
        print(f"Check if Table: {tbl_exclusionColumnAttributeList} exists or not in catalog explorer")

    finally:
        ic(columns_to_exclude_from_comparison)
        return columns_to_exclude_from_comparison
