# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, count, rank, row_number, unix_timestamp
from walrus import *

import pandas as pd
import yaml

# Naming Constants
key_joiner = ':'

recent_key_meta = 'recent'
count_key_meta = 'count'
color_key_meta = 'color'

# Application Constants
with open('config.yaml', 'r') as f:
  config = yaml.load(f)

recent_data_count = config['recent_data_count']
color_data_count = config['color_data_count']

required_columns = config['required_columns']

# Connecting Redis Database
db = Database(host = config['redisdb']['host'], port = config['redisdb']['port'], db = config['redisdb']['db'])

# To execute operations in cluster
spark = SparkSession \
                  .builder \
                  .appName('analytics') \
                  .master("local[*]") \
                  .getOrCreate();

# Loading data as spark DF. Column with dots -> Upgrade pyspark > 2.0.0
session_df = spark.read \
                  .option("delimiter", ",") \
                  .option("inferSchema", "true") \
                  .option("header", config['data_headers']) \
                  .csv(config['data_path'])

# Data clean up

# # REMOVE DUPLICATE RECORDS AND IF A COLUMN HAS NULL VALUE
# clean_df = session_df.dropna() \
#                     .dropDuplicates() \
#                     .select(required_columns)
min_df = session_df.select(required_columns)
clean_df = min_df.dropna()

# API II - /getBrandsCount

count_df = clean_df.withColumn('date_added', unix_timestamp(clean_df.dateAdded.cast('date'))) \
                    .groupBy('date_added', 'brand') \
                    .agg(count('brand')) \
                    .orderBy('date_added', 'count(brand)', ascending=False)

count_dict = count_df.toPandas() \
                      .groupby('date_added') \
                      .apply(lambda x: dict(zip(x['brand'], x['count(brand)']))) \
                      .to_dict()

for epoch_date, data in count_dict.iteritems():
    count_key = count_key_meta + key_joiner + str(epoch_date)

    if db.exists(count_key):
        count_hash = db.get_key(count_key)
    else:
        count_hash = db.Hash(count_key)

    count_hash.update(data)

# API III - /getItemsbyColor

color_window = Window.partitionBy(clean_df.colors) \
                    .orderBy(clean_df.dateAdded.desc(), clean_df.dateUpdated.desc())

color_df = clean_df.select('*', row_number() \
                                .over(color_window) \
                                .alias('row_number')) \
                    .filter(col('row_number') <= color_data_count) \
                    .drop('row_number')

color_dict = color_df.toPandas() \
                    .groupby(['colors']) \
                    .apply(lambda x: x.to_dict('records'))

for color, data in color_dict.iteritems():
    split_colors = color.split(',')

    for split_color in split_colors:
        color_key = (color_key_meta + key_joiner + split_color).lower()

        if db.exists(color_key):
            color_hash = db.get_key(color_key)
        else:
            color_hash = db.List(color_key)

        color_hash.extend(data)