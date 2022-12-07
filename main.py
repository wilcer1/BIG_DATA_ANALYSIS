from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan,when,count, collect_list
from datetime import datetime, date
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName('demo').master('local').enableHiveSupport().getOrCreate()

df = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/sample/dataset/1987.csv")

print(df.columns)


# hi = df.select(collect_list("CRSElapsedTime")).first()[0]

# function to check for NA values
def check_na(df):
    for col in df.columns:
        print(col,df.filter(df[col] == "NA").count())

check_na(df)






