from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('demo').master('local').enableHiveSupport().getOrCreate()

df = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9870/sample/dataset/1987.csv")
df.printSchema()
df.show()


