from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()

#load dataframe from csv
df = spark.read.csv('.csv', header=True, inferSchema=True)


