from pyspark.sql import SparkSession
import yaml

class SparkSetup:

  with open('config/spark.yaml', 'r') as f:
    config = yaml.load(f)

  # To execute operations in cluster
  spark = SparkSession \
            .builder \
            .appName(config['appname']) \
            .master("local[*]") \
            .getOrCreate();

  data_path = config['data_path']
  data_headers = config['data_headers']