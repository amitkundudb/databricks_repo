# Databricks notebook source
# MAGIC %run /Users/kundu7337@gmail.com/assignment/source_to_bronze/utils
# MAGIC

# COMMAND ----------

employee_df = spark.read.format("delta").load('/FileStore/assignment/questoin1/silver/employee_info/dim_employee')

# COMMAND ----------

employee_df.write.format("parquet").mode("overwrite").option("replaceWhere","load_date = '2024-04-16'").save("/FileStore/assignment/gold/employee/fact_employee")