# Databricks notebook source
# MAGIC %run "/Users/kundu7337@gmail.com/assignment/source_to_bronze/utils"

# COMMAND ----------

#Reading Datasets as dataframe

read_custom_schema(country_data,"/FileStore/assignment/resource/country_q1.csv")

read_custom_schema(employee_data, "/FileStore/assignment/resource/employee_q1.csv")

read_custom_schema(department_data, "/FileStore/assignment/resource/department_q1.csv")

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

# COMMAND ----------

