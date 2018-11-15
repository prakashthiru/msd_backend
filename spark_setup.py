from pyspark.sql import SparkSession
import yaml
import os

class SparkSetup:

  with open(os.path.join(os.path.dirname(__file__), 'config/app.yaml')) as f:
    config = yaml.load(f)

  # To execute operations in cluster
  spark = SparkSession \
            .builder \
            .appName(config['appname']) \
            .master("local[*]") \
            .getOrCreate();

  data_path = config['data_path']
  data_headers = config['data_headers']
