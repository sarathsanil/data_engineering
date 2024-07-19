convert_files_to_dataframe('/mnt/audeusdlsdevkcwgaap01-analysis-rules-sarath/BugFix/Tucana/05SEP2023/h-5388D992-4FB6-4E3C-96FB-12EF8FD80BD5/','cdm-l1')

fin_L1_MD_GLAccount = fin_L1_MD_GLAccount.select(col('CompanyCode'))

fin_L1_MD_GLAccount.printSchema()

reports.display()


result = spark.sql("SELECT * FROM fin_L1_MD_GLAccount")
result.show()