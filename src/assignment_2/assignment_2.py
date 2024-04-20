# Databricks notebook source
import requests
import json
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, current_date

def create_df(json_data,custom_schema):
    raw_df = spark.createDataFrame(json_data,schema = custom_schema)
    return raw_df

def col_drop(raw_df):
    df = raw_df.drop('page', 'per_page', 'total', 'total_pages', 'support')
    return df

def flatten(df):
    flatten_df = df.withColumn('data', explode('data'))
    return flatten_df

def new_col(flatten_df):
    newcol_df = flatten_df.withColumn("id", flatten_df.data.id).withColumn('email', flatten_df.data.email).withColumn('first_name', flatten_df.data.first_name).withColumn('last_name', flatten_df.data.last_name).withColumn('aatar', flatten_df.data.avatar).drop(flatten_df.data)
    return new_coldf

def derived_df(new_coldf):
    derived_site_address_df = new_coldf.withColumn("site_address",split(new_coldf["email"],"@")[1])
    return derived_site_address_df

def load_date(derived_site_address_df):
    loaded_date = derived_site_address_df.withColumn('load_date', current_date())
    return loaded_date

def testing(path):
    loaded_date.write.format('delta').mode('overwrite').save(path)
    testing_df = spark.read.format('delta').load(path)
    return testing_df
