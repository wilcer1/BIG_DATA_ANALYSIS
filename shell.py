import cmd
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import matplotlib.ticker as ticker


class Shell(cmd.Cmd):

    def do_setup(self, line):
        self.spark = SparkSession.builder.appName('demo').master('local').enableHiveSupport().getOrCreate()
        self.starting_point = 1987
        self.ending_point = 2009
        self.df = {}
        load_files(self)
        

    def load_files(self):
        for i in range(self.starting_point,self.ending_point):
            self.df[str(i)] = self.spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/sample/dataset/"+str(i)+".csv")
            print(i, " files loaded")
    
    
    def do_plot_null_values(self, line):
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

    def do_greet(self, line):
        print("hello, world")

    def do_EOF(self, line):
        return True
    
   
        



shell = Shell()
shell.cmdloop()