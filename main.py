
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import matplotlib.ticker as ticker

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

def plot_null_value_of_file(file, name):
    null_counts = file.select(*(F.sum(F.isnan(F.col(c)).cast("int")).alias(c) for c in file.columns)).toPandas().iloc[0]
    plt.figure(figsize=(10, 15))
    plt.xticks(rotation=90)
    plt.bar(null_counts.index, null_counts.values)
    for col, count in null_counts.items():
        print(f"Column '{col}' has {count} null values")
    print("name: ", name)
    plt.xlabel("Columns")
    plt.ylabel("Number of null values")
    plt.title("Distribution of null values across columns")
        
    plt.savefig(f"img\\NA_values_{name}.png")
    plt.clf()

def plot_air_delay():
    plt.figure(figsize=(10, 10))
    max_delay = 0
    file_names = []
    for name, file in df.items():
        
        # get the list of delays
        delays = file.select("ArrDelay").toPandas()["ArrDelay"].tolist()
        # filter out the NA values
        delays_filtered = [d for d in delays if d != 'NA']
        # convert the list of strings to integers and find max delay
        max_delay = max(max_delay, max(map(int, delays_filtered)))
        plt.plot(delays, label=name)
        
        file_names.append(name)
    
    plt.xlabel("Year")
    plt.ylabel("Air Delay (minutes)")
    plt.title("Air Delay over Time")
    plt.legend()
    # set the y-axis ticks to be every 60 minutes and the max value to be the max delay
    plt.yticks(range(0, max_delay+1, 60))

    # Set the tick labels to the file names using a FixedFormatter
    formatter = ticker.FixedFormatter(file_names)
    plt.gca().xaxis.set_major_formatter(formatter)
    
    
    # Save the plot to a new file
    plt.savefig(f"img\\delay\\air_delay_comparison.png")
    plt.clf()



def main():
    plot_air_delay()
    
   


if __name__ == "__main__":
    main()



