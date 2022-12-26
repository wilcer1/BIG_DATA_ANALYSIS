from pyspark.sql import SparkSession

import pandas as pd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('demo').master('local').enableHiveSupport().getOrCreate()

df = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/sample/dataset/1987.csv")




starting_point = 1987
ending_point = 2009
df = {}
pandasDF = {}

for i in range(starting_point,ending_point):
    df[str(i)] = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/sample/dataset/"+str(i)+".csv")
    

# convert all pyspark dataframes to pandas dataframes
def convert_to_pandas():
    for name, file in df.items():
        pandasDF[name] = file.toPandas()
    print("converted to pandas")

# convert all NA values to NaN
def na_to_nan():
    for file in df:
        df[file].replace("NA", np.nan)
    print("converted to nan")



def replace_na(dataframe_dict):
    for key, val in dataframe_dict.items():
        val = val.replace("NA", str(np.nan))
        dataframe_dict[key] = val
    return dataframe_dict

# function to check for NaN values in the dataset and plot them
def check_nan():
    for name, file in df.items():
        for col in file.columns:
            
            plt.title(f"NA values in {name} dataset")
            plt.xlabel("Columns")
            plt.ylabel("Number of NA values")
            plt.savefig(f"/img/NA_values_{name}.png")

def plot_null_values():
    for name, file in df.items():
        null_counts = file.select(*(F.sum(F.isnan(F.col(c)).cast("int")).alias(c) for c in file.columns)).toPandas().iloc[0]
        plt.figure(figsize=(10, 15))
        plt.xticks(rotation=90)
        plt.bar(null_counts.index, null_counts.values)
        print("name: ", name)
        plt.xlabel("Columns")
        plt.ylabel("Number of null values")
        plt.title("Distribution of null values across columns")
        
        plt.savefig(f"img\\NA_values_{name}.png")
        plt.clf()

def plot_air_delay():
    plt.figure(figsize=(10, 10))
    max_delay = 0
    for name, file in df.items():
        
        delays = file.select("ArrDelay").toPandas()["ArrDelay"].tolist()
        max_delay = max(max_delay, max(delays))
        plt.plot(delays, label=name)
    
    plt.xlabel("Time")
    plt.ylabel("Air Delay (minutes)")
    plt.title("Air Delay over Time")
    plt.legend()
    plt.yticks(range(0, max_delay+1, 10))
    
    
    # Save the plot to a new file
    plt.savefig(f"img\\delay\\air_delay_{i}.png")
    plt.clf()



def main():
    plot_air_delay()
   


if __name__ == "__main__":
    main()



