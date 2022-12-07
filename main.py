import os
import pyspark
os.environ['SPARK_HOME'] = 'C:\spark\spark-3.0.1-bin-hadoop2.7'

from pyspark.sql import SparkSession

# Create a SparkSession object
spark = SparkSession.builder.appName("MyApp").getOrCreate()
