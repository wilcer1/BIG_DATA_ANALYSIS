import cmd
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import matplotlib.ticker as ticker


class Shell(cmd.Cmd):
    def start_loop(self):
        self.setup()
        self.cmdloop()

    def setup(self):
        self.spark = SparkSession.builder.appName('demo').master('local').enableHiveSupport().getOrCreate()
        self.starting_point = 1987
        self.ending_point = 2009
        self.df = {}
        for i in range(self.starting_point,self.ending_point):
            self.df[str(i)] = self.spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/sample/dataset/"+str(i)+".csv")
            print(i, " file loaded")
        
    
    
    
    
    def do_plot_null_values(self, line):
        """Plot null values in the dataset and save them to img folder"""
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
        """Plot the average air delay over time"""
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

        # Set the tick labels to the file names using a FixedFormatter
        formatter = ticker.FixedFormatter(file_names)
        plt.gca().xaxis.set_major_formatter(formatter)
        
        
        # Save the plot to a new file
        plt.savefig(f"img\\delay\\air_delay_comparison.png")
        plt.clf()
    
    def plot_cancelled():
        """Plot the average number of cancelled flights over time"""
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
  

    def do_EOF(self, line):
        """Exit the shell"""
        return True
    
   
        


shell = Shell()
shell.start_loop()