# Databricks notebook source
# MAGIC %run /Users/kundu7337@gmail.com/assignment/source_to_bronze/utils
# MAGIC

# COMMAND ----------

employee_df = spark.read.format("delta").load('/FileStore/assignment/questoin1/silver/employee_info/dim_employee')
display(employee_df)

# COMMAND ----------

display(salary_of_each_department(employee_df))


display(employee_count(employee_df))


display(list_the_department(employee_df))



display(avg_age(employee_df))

display(add_load_date(employee_df))

# COMMAND ----------

employee_df.write.format("parquet").mode("overwrite").option("replaceWhere","load_date = '2024-04-16'").save("/FileStore/assignment/gold/employee/fact_employee")

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/assignment/src/assignment_2")