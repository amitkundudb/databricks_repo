# Databricks notebook source
# MAGIC %run "/Users/kundu7337@gmail.com/assignment/assignment_2"

# COMMAND ----------

# MAGIC %pip install nutter

# COMMAND ----------

import requests
import unittest
from src.assignment_2.utils import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, MapType


class TestSparkFunctions(unittest.TestCase):
    def test_create_df(self):
        response_data = requests.get('https://reqres.in/api/users?page=2')
        data = response_data.json()['data']
        json_schema = StructType([
            StructField("page", IntegerType(), True),
            StructField("per_page", IntegerType(), True),
            StructField("total", IntegerType(), True),
            StructField("total_pages", IntegerType(), True),
            StructField("data", ArrayType(StructType([
                StructField("id", IntegerType(), True),
                StructField("email", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("avatar", StringType(), True)
            ])), True),
            StructField("support", StructType([
                StructField("url", StringType(), True),
                StructField("text", StringType(), True)
            ]), True)
        ])
        expected_df = spark.createDataFrame(data, schema=json_schema)
        raw_df = create_df(data, json_schema)
        self.assertListEqual(raw_df.collect(), expected_df.collect())
        return raw_df

    def test_col_drop(self):
        raw_df = self.test_create_df()
        expected_df = raw_df.drop('page', 'per_page', 'total', 'total_pages', 'support')
        col_dropped_df = col_drop(raw_df)
        self.assertListEqual(col_dropped_df.collect(), expected_df.collect())
        return col_dropped_df

    def test_flatten(self):
        col_dropped_df = self.test_col_drop()
        expected_df = col_dropped_df.withColumn('data', explode(col_dropped_df['data']))
        flatten_df = flatten(col_dropped_df)
        self.assertListEqual(flatten_df.collect(), expected_df.collect())
        return flatten_df

    def test_new_col(self):
        flatten_df = self.test_flatten()
        expected_df = flatten_df.withColumn("id", flatten_df.data.id).withColumn('email',
                                                                                 flatten_df.data.email).withColumn(
            'first_name', flatten_df.data.first_name).withColumn('last_name', flatten_df.data.last_name).withColumn(
            'aatar', flatten_df.data.avatar).drop(flatten_df.data)
        new_df = new_col(flatten_df)
        self.assertListEqual(new_df.collect(), expected_df.collect())
        return new_df

    def test_derived_df(self):
        new_df = self.test_new_col()
        expected_df = new_df.withColumn("site_address", split(new_df["email"], "@")[1])
        derived_mail_address_df = derived_df(new_df)
        self.assertListEqual(derived_mail_address_df.collect(), expected_df.collect())
        return derived_mail_address_df

    def test_load_date(self):
        derived_mail_address_df = self.test_derived_df()
        expected_df = derived_mail_address_df.withColumn('load_date', current_date())
        load_date_df = load_date(derived_mail_address_df)
        self.assertListEqual(load_date_df.collect(), expected_df.collect())
        return load_date_df

# As Pycharm don't support "delta" format, Not considering this testcase
    # def test_testing(self):
    #     load_date_df = self.test_load_date()
    #     path = '/test/'
    #     load_date_df.write.format('delta').mode('overwrite').save(path)
    #     expected_df = spark.read.format('delta').load(path)
    #     testing_readwrite_df = testing(load_date_df, path)
    #     self.assertListEqual(testing_readwrite_df.collect(), expected_df.collect())


if __name__ == '__main__':
    unittest.main()
