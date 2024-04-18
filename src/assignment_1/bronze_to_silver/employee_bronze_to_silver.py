# Databricks notebook source
# MAGIC %run /Users/kundu7337@gmail.com/assignment/source_to_bronze/utils

# COMMAND ----------

# MAGIC %run "/Users/kundu7337@gmail.com/assignment/source_to_bronze/schema"

# COMMAND ----------


emp_path = '/FileStore/assignment/resource/employee_q1.csv'
employee_df = read_custom_schema(emp_path , employee_schema)

# COMMAND ----------


dep_path = '/FileStore/assignment/resource/department_q1.csv'

department_df = read_custom_schema(dep_path , department_schema)

# COMMAND ----------


country_path = '/FileStore/assignment/resource/country_q1.csv'
country_df = read_custom_schema(country_path , country_schema)


# COMMAND ----------


employee_snake_case = camel_to_snake(employee_df)

# COMMAND ----------

date_load = current_date_df(employee_snake_case)

# COMMAND ----------



 spark.sql('CREATE DATABASE employee_info')
 spark.sql('use employee_info')


# COMMAND ----------

employee_df.write.format("delta").option('path', '/FileStore/assignment/questoin1/silver/employee_info/dim_employee').mode("overwrite").saveAsTable('dim_employee')