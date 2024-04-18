# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


department_schema = StructType([
    StructField("DepartmentID", StringType(), True),
    StructField("DepartmentName", StringType(), True)
])

employee_schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("DepartmentID", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Salary", DoubleType(), True),
    StructField("Age", IntegerType(), True)
])
country_schema = StructType([
    StructField("CountryCode", StringType(), True),
    StructField("CountryName", StringType(), True)
])
