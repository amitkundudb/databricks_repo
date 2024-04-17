# Databricks notebook source
# MAGIC %run /Users/kundu7337@gmail.com/assignment/source_to_bronze/utils

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

# Read file with custom schema
custom_schema = StructType([
    StructField("emp_Id", StringType(), True),
    StructField("emp_Name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("Country", StringType(),  True),
    StructField("Salary", StringType(), True),
    StructField("Age", StringType(), True),
])

emp_path = '/FileStore/assignment/resource/employee_q1.csv'
employee_df = read_custom_schema(emp_path , custom_schema)
employee_df.show()
employee_df.printSchema()

# COMMAND ----------


schema = StructType([
    StructField("dep_id", StringType(), True),
    StructField("dep_name", StringType(), True)
])

dep_path = '/FileStore/assignment/resource/department_q1.csv'

department_df = read_custom_schema(dep_path , schema)

department_df.show()
department_df.printSchema()

# COMMAND ----------

schema=schema = StructType([
    StructField("country_code", IntegerType(), True),
    StructField("country_name", StringType(), True)
])

country_path = '/FileStore/assignment/resource/country_q1.csv'
country_df = read_custom_schema(country_path , schema)
country_df.show()
country_df.printSchema()


# COMMAND ----------


employee_snake_case = camel_to_snake(employee_df)
display(employee_snake_case)

# COMMAND ----------

date_load = current_date_df(employee_snake_case)
display(date_load)

# COMMAND ----------



 spark.sql('CREATE DATABASE employee_info')
 spark.sql('use employee_info')


# COMMAND ----------

employee_df.write.option('path', 'dbfs:/FileStore/assignment/questoin1/silver/employee_info/dim_employee').saveAsTable('dim_employee')