
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from graphframes import GraphFrame

import matplotlib.ticker as ticker

spark = SparkSession.builder.appName('demo').master('local').enableHiveSupport().getOrCreate()






starting_point = 1987
ending_point = 2009
df = {}
pandasDF = {}
x = 0
for i in range(starting_point,ending_point):
    df[str(i)] = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/sample/dataset/"+str(i)+".csv")
    print(x)
    x+=1
    

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
    plt.figure(figsize=(15, 10))
    avg_delays = {}
    file_names = []
    max_avg = 0
    i = 0
    for name, file in df.items():
        print("name: ", name, f"{i}/{len(df)}")
        i+=1
        
        # get the list of delays
        delays = file.select("ArrDelay")
        # filter out the NA values
        avg_delay = delays.agg(F.avg("ArrDelay"))
        # get the average delay of the file
        avg_delay = avg_delay.first()[0]
        max_avg = max(max_avg, avg_delay)
        avg_delays[name] = avg_delay
        file_names.append(name)
    
    plt.plot(file_names, avg_delays.values())
    plt.xlabel("Year")
    plt.ylabel("Arr Delay (minutes)")
    plt.title("Arr Delay over Time")
    # plt.legend()
    # set the y-axis ticks to be every 60 minutes and the max value to be the max delay
    plt.ylim(0, round(max(avg_delays.values()))+1)

    # Set the y-axis ticks
    plt.yticks(range(0, round(max(avg_delays.values()))+1))
    print(round(max_avg))
    plt.xticks(range(0, len(file_names), 1))


# function to plot column "Cancelled" for each file
def plot_cancelled():
    plt.figure(figsize=(20, 20))
    cancelled = {}
    file_names = []
    max_cancelled = 0
    i = 0
    for name, file in df.items():
        print("name: ", name, f"{i}/{len(df)}")
        i+=1
        
        # get the list of delays
        cancelled_flights = file.select("Cancelled")
        # filter out the NA values
        cancelled_flights = cancelled_flights.agg(F.sum("Cancelled"))
        # get the average delay of the file
        cancelled_flights = cancelled_flights.first()[0]
        max_cancelled = max(max_cancelled, cancelled_flights)
        cancelled[name] = cancelled_flights
        file_names.append(name)
    
    plt.plot(file_names, cancelled.values())
    plt.xlabel("Year")
    plt.ylabel("Cancelled Flights")
    plt.title("Cancelled Flights over Time")
    # plt.legend()
    # set the y-axis ticks to be every 60 minutes and the max value to be the max delay
    plt.ylim(0, round(max(cancelled.values()))+1)

    # Set the y-axis ticks
    plt.yticks(range(0, round(max(cancelled.values()))+1, 5000))
    print(round(max_cancelled))
    plt.xticks(range(0, len(file_names), 1))

    # Set the tick labels to the file names using a FixedFormatter
    formatter = ticker.FixedFormatter(file_names)
    plt.gca().xaxis.set_major_formatter(formatter)
    
    
    # Save the plot to a new file
    plt.savefig(f"img\\cancelled\\cancelled_comparison.png")
    plt.clf()


    


def plot_arr_delay(df_dict):
  
  vertices = []
  edges = []

  # Loop through the DataFrames in the dictionary
  for i, (filename, df) in enumerate(df_dict.items()):
    # Filter out "NA" values and negative values from the ArrDelay column
    df = df.dropna(subset=['ArrDelay'])
    df = df.where(df['ArrDelay'] > 0)

    # Add a vertex for the current DataFrame
    vertices.append((i, filename))

    # Add an edge for each value in the ArrDelay column
    for row in df.select('ArrDelay').collect():
      edges.append((i, row[0]))

  # Create a GraphFrame from the vertices and edges
  g = GraphFrame(spark.createDataFrame(vertices, ["id", "filename"]),
                 spark.createDataFrame(edges, ["src", "dst"]))

   # Convert the GraphFrame to a Pandas DataFrame
  g_pandas = g.toPandas()

  # Plot the graph using Matplotlib
  ax = g_pandas.plot()

  # Set the x-axis label
  ax.set_xlabel('Filename')

  # Set the y-axis label
  ax.set_ylabel('Arrival Delay (minutes)')

  # Save the plot to a file
  plt.savefig("img\\delay\\air_delay_comparison.png")
 


def main():
    plot_cancelled()
    
    
   


if __name__ == "__main__":
    main()



