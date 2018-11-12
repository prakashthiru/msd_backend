from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import yaml

with open('config.yaml', 'r') as f:
        config = yaml.load(f)

class Spark_Context:
  conf = SparkConf().setAppName(config['appname'])
  sc = SparkContext(conf=conf)
  sqlContext = SQLContext(sc)