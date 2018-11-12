from pyspark.sql import Row
from walrus import *

import spark_context
import pandas as pd
import csv

sc = spark_context.Spark_context.sc
sqlContext = spark_context.Spark_context.sqlContext

class Stats:

  def create_dataframe(self, file_path, header = True):
    dfs = sqlContext.read.csv(file_path, header = header)