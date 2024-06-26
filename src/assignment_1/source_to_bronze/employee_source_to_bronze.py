# Databricks notebook source
# MAGIC %run "/Users/kundu7337@gmail.com/assignment/source_to_bronze/utils"

# COMMAND ----------

# MAGIC %run "/Users/kundu7337@gmail.com/assignment/source_to_bronze/schema"

# COMMAND ----------

#Reading Datasets as dataframe

country_data = read_custom_schema("/FileStore/assignment/resource/country_q1.csv",country_schema)

employee_data =read_custom_schema("/FileStore/assignment/resource/employee_q1.csv",employee_schema)

department_data =read_custom_schema("/FileStore/assignment/resource/department_q1.csv",department_schema)

# country_data = spark.read.format("csv").options(header = True).load("/FileStore/assignment/resource/country_q1.csv")

# employee_data = spark.read.format("csv").options(header = True).load("/FileStore/assignment/resource/employee_q1.csv")

# department_data = spark.read.format("csv").options(header = True).load("/FileStore/assignment/resource/department_q1.csv")

# COMMAND ----------

#Writing the files in DBFS

write_csv(country_data, "/source_to_bronze/country_q1.csv")

write_csv(employee_data, "/source_to_bronze/employee_q1.csv")

write_csv(department_data, "/source_to_bronze/department_q1.csv")

# country_data.write.format("csv").mode("ignore").options(header = True).save("/source_to_bronze/country_q1.csv")

# employee_data.write.format("csv").mode("ignore").options(header = True).save("/source_to_bronze/employee_q1.csv")

# department_data.write.format("csv").mode("ignore").options(header = True).save("/source_to_bronze/department_q1.csv")