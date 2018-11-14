# coding: utf-8
from pyspark.sql import Window, Row
from pyspark.sql.functions import col, count
from pyspark.sql.functions import rank, row_number
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import LongType

import redis
import pandas as pd
import app_constants
import spark_setup
import database_setup

# Setups
db = database_setup.DatabaseSetup.db
spark = spark_setup.SparkSetup.spark

# Loading data as spark DF. Column with dots -> Upgrade pyspark > 2.0.0
session_df = spark.read \
                  .option("delimiter", ",") \
                  .option("inferSchema", "true") \
                  .option("header", spark_setup.SparkSetup.data_headers) \
                  .csv(spark_setup.SparkSetup.data_path)

# Data clean up: REMOVE duplicate records and if required column has NULL value
clean_df = session_df.drop_duplicates() \
                  .dropna(subset=app_constants.Columns.REQUIRED) \
                  .select(app_constants.Columns.REQUIRED)

def generate_stats():
  count_stats()
  color_stats()
  recent_stats()

  # API I - /getRecentItem
def recent_stats():
  date_window = Window.partitionBy(clean_df.dateAdded) \
                      .orderBy(clean_df.dateAdded.desc(),
                              clean_df.dateUpdated.desc())

  recent_df = clean_df.withColumn('date_timestamp',
                        unix_timestamp(clean_df.dateAdded.cast('date'))) \
                      .withColumn("date_added", unix_timestamp(clean_df.dateAdded)) \
                      .withColumn("date_updated", unix_timestamp(clean_df.dateUpdated)) \
                      .withColumn('row_number', row_number().over(date_window)) \
                      .filter(col('row_number') == app_constants.Count.RECENT_DATA) \
                      .drop('row_number', 'dateAdded', 'dateUpdated') \
                      .withColumnRenamed('date_added', 'dateAdded') \
                      .withColumnRenamed('date_updated', 'dateUpdated')

  recent_dict = recent_df.toPandas().to_dict('records')

  for data in recent_dict:
    # Create key with recent namespace & epoch date
    recent_key = app_constants.KeyMeta.RECENT + app_constants.KeyMeta.JOINER + str(data['date_timestamp'])
    # Assigning entire hash to the key
    db.hmset(recent_key, data)

# API II - /getBrandsCount
def count_stats():
  count_df = clean_df.withColumn('date_timestamp', \
                          unix_timestamp(clean_df.dateAdded.cast('date'))) \
                    .groupBy('date_timestamp', 'brand') \
                    .agg(count('brand')) \
                    .orderBy('date_timestamp', 'count(brand)', ascending=False)

  count_dict = count_df.toPandas() \
                    .groupby('date_timestamp') \
                    .apply(lambda x: dict(zip(x['brand'], x['count(brand)']))) \
                    .to_dict()

  for epoch_date, data in count_dict.iteritems():
    # Create key with count namespace & epoch date
    count_key = app_constants.KeyMeta.COUNT + app_constants.KeyMeta.JOINER + str(epoch_date)
    # Assigning entire hash to the redis key
    db.hmset(count_key, data)

# API III - /getItemsbyColor
def color_stats():
  color_window = Window.partitionBy(clean_df.colors) \
                      .orderBy(clean_df.dateAdded.desc(), \
                              clean_df.dateUpdated.desc())

  color_df = clean_df.select('*', row_number() \
                              .over(color_window) \
                              .alias('row_number')) \
                    .filter(col('row_number') <= app_constants.Count.COLOR_DATA) \
                    .withColumn('dateAdded', unix_timestamp(clean_df.dateAdded).cast(LongType())) \
                    .withColumn('dateUpdated', unix_timestamp(clean_df.dateUpdated).cast(LongType())) \
                    .drop('row_number')

  color_dict = color_df.toPandas() \
                      .groupby(['colors']) \
                      .apply(lambda x: x.to_dict('records'))

  # Drop the list value if the key already holds a value
  for k, v in color_dict.iteritems():
    for color in k.split(','):
      key = (app_constants.KeyMeta.COLOR + app_constants.KeyMeta.JOINER + color).lower()
      if db.exists(key): db.delete(key)

  # Inspect the color values and push them to the key's list
  for color, data in color_dict.iteritems():
    split_colors = color.split(',')

    for split_color in split_colors:
      color_key = (app_constants.KeyMeta.COLOR + app_constants.KeyMeta.JOINER + split_color).lower()
      db.rpush(color_key, *data)

if __name__=="__main__":
  generate_stats()