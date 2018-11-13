# coding: utf-8
from pyspark.sql import Window, Row
from pyspark.sql.functions import col, count
from pyspark.sql.functions import rank, row_number
from pyspark.sql.functions import unix_timestamp
from walrus import *

import pandas as pd
import app_constants
import spark_setup
import database_setup

# Setups
db = database_setup.DatabaseSetup.db
spark = spark_setup.SparkSetup.spark

if __name__=="__main__":
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

    recent_df = clean_df.withColumn('date_added',
                              unix_timestamp(clean_df.dateAdded.cast('date'))) \
                        .withColumn('row_number', row_number().over(date_window)) \
                        .filter(col('row_number') == app_constants.Count.RECENT_DATA) \
                        .drop('row_number')

    recent_dict = recent_df.toPandas().to_dict('records')

    for data in recent_dict:
        recent_key = app_constants.KeyMeta.RECENT + app_constants.KeyMeta.JOINER + str(data['date_added'])

        if db.exists(recent_key):
            recent_hash = db.get_key(recent_key)
        else:
            recent_hash = db.Hash(recent_key)

        recent_hash.update(data)

  # API II - /getBrandsCount
  def count_stats():
    count_df = clean_df.withColumn('date_added', \
                            unix_timestamp(clean_df.dateAdded.cast('date'))) \
                      .groupBy('date_added', 'brand') \
                      .agg(count('brand')) \
                      .orderBy('date_added', 'count(brand)', ascending=False)

    count_dict = count_df.toPandas() \
                      .groupby('date_added') \
                      .apply(lambda x: dict(zip(x['brand'], x['count(brand)']))) \
                      .to_dict()

    for epoch_date, data in count_dict.iteritems():
        count_key = app_constants.KeyMeta.COUNT + app_constants.KeyMeta.JOINER + str(epoch_date)

        print count_key
        if db.exists(count_key):
            count_hash = db.get_key(count_key)
        else:
            count_hash = db.Hash(count_key)

        count_hash.update(data)

  # API III - /getItemsbyColor
  def color_stats():
    color_window = Window.partitionBy(clean_df.colors) \
                        .orderBy(clean_df.dateAdded.desc(), \
                                clean_df.dateUpdated.desc())

    color_df = clean_df.select('*', row_number() \
                                    .over(color_window) \
                                    .alias('row_number')) \
                        .filter(col('row_number') <= app_constants.Count.COLOR_DATA) \
                        .drop('row_number')

    color_dict = color_df.toPandas() \
                        .groupby(['colors']) \
                        .apply(lambda x: x.to_dict('records'))

    for color, data in color_dict.iteritems():
        split_colors = color.split(',')

        for split_color in split_colors:
            color_key = (app_constants.KeyMeta.COLOR + app_constants.KeyMeta.JOINER + split_color).lower()

            if db.exists(color_key):
                color_hash = db.get_key(color_key)
            else:
                color_hash = db.List(color_key)

            color_hash.extend(data)

  generate_stats()