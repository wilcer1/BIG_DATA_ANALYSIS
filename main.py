
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

def plot_ArrDelay_values():
    neg_nums = 0
    total_nums = 0
    for name, file in df.items():
        print()
        # get the list of delays
        delays = file.select("ArrDelay").toPandas()["ArrDelay"].tolist()
        for i in delays:
            total_nums += 1
            if i != "NA" and int(i) < 0 :
                neg_nums += 1
        print(f"Number of negative values in {name} dataset: {neg_nums}/{total_nums}")



def plot_arr_delay():
    plt.figure(figsize=(15, 10))
    max_delay = 0
    file_names = []
    for i, (name, file) in enumerate(df.items()):
        print(f"Plotting {name} dataset, {i+1}/{len(df)}")
        # get the list of delays
        delays = file.select("ArrDelay").toPandas()["ArrDelay"].tolist()
        
        # filter out the NA values
        delays_filtered = [d for d in delays if d != 'NA' and int(d) > 0]
        # convert the list of strings to integers and find max delay
        # max_delay = max(max_delay, max(map(int, delays_filtered)))
        delays_filtered.sort()
        plt.plot(delays_filtered, label=name)
        
        file_names.append(name)
    
    plt.xlabel("Year")
    plt.ylabel("Air Delay (minutes)")
    plt.title("Air Delay over Time")
    plt.legend()
    # set the y-axis ticks to be every 60 minutes and the max value to be the max delay
    plt.yticks(ticks = range(0, max_delay+1, 60))

    plt.ylim(delays_filtered[0], delays_filtered[-1])
    
    plt.xticks(ticks = range(0, len(file_names), 1), labels=file_names)
    
    


    # Set the tick labels to the file names using a FixedFormatter
    formatter = ticker.FixedFormatter(file_names)
    plt.gca().xaxis.set_major_formatter(formatter)
    
    
    # Save the plot to a new file
    plt.savefig(f"img\\delay\\air_delay_comparison.png")
    plt.clf()



def main():
    plot_arr_delay()
    
   


if __name__ == "__main__":
    main()



