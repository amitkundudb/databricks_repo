# Databricks notebook source
# MAGIC %run "/Users/kundu7337@gmail.com/assignment/assignment_2"

# COMMAND ----------

import requests
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, MapType
from runtime.nutterfixture import NutterFixture, tag

class MyTestFixture(NutterFixture):
    def assertion_create_df(self):
        response_data = requests.get('https://reqres.in/api/users?page=2')
        data = response_data.json()['data']
        json_schema = StructType([
            StructField("page", IntegerType(), True),
            StructField("per_page", IntegerType(), True),
            StructField("total", IntegerType(), True),
            StructField("total_pages", IntegerType(), True),
            StructField("data", StructType([
                StructField("id", IntegerType(), True),
                StructField("email", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("avatar", StringType(), True)
            ]), True),
            StructField("support", StructType([
                StructField("url", StringType(), True),
                StructField("text", StringType(), True)
            ]), True)
        ])
        expected_df = spark.createDataFrame(data,schema=json_schema)
        actual_df = create_df(data,json_schema)
        assert actual_df.collect() == expected_df.collect()
    def assertion_col_drop(self):
        response_data = requests.get('https://reqres.in/api/users?page=2')
        json_data = response_data.json()
        data_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("avatar", StringType(), True)
        ])
        custom_schema = StructType([
            StructField("page", IntegerType(), True),
            StructField("per_page", IntegerType(), True),
            StructField("total", IntegerType(), True),
            StructField("total_pages", IntegerType(), True),
            StructField("data", ArrayType(data_schema), True),
            StructField("support", MapType(StringType(), StringType()), True)
        ])
        actual_df = spark.createDataFrame([json_data], custom_schema)
        expected_df = actual_df.drop('page', 'per_page', 'total', 'total_pages', 'support')
        result_df = col_drop(actual_df)
        assert result_df.collect() == expected_df.collect()


result = MyTestFixture().execute_tests()
print(result.to_string())