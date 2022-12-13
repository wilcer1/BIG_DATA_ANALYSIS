from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan,when,count, collect_list
from datetime import datetime, date
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName('demo').master('local').enableHiveSupport().getOrCreate()

df = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/sample/dataset/1987.csv")


# hi = df.select(collect_list("CRSElapsedTime")).first()[0]


starting_point = 1987
ending_point = 2008
df = {}
pandasDF = {}

for i in range(starting_point,ending_point):
    df[str(i)] = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/sample/dataset/"+str(i)+".csv")
    

# function to check for NA values in the dataset
def check_na(df):
    with open("NA_values.txt", "w") as f:
        for name, file in df.items():
            f.writelines("Year: "+name+"\n")
            for col in file.columns:
                f.writelines("Total NA values: " + col + " " + str(file.filter(file[col] == "NA").count())+"\n")
                f.writelines((" NA values / total file values" + " " + col + " " + str(file.filter(file[col]== "NA").count()/file.count()))+"\n")
                


# convert all pyspark dataframes to pandas dataframes
def convert_to_pandas():
    for name, file in df.items():
        pandasDF[name] = file.toPandas()
    print("converted to pandas")

# convert all NA values to NaN
def na_to_nan():
    for file in pandasDF:
        pandasDF[file].replace("NA", pd.np.nan, inplace=True)
    print("converted to nan")

# function to check for NaN values in the dataset and plot them
def check_nan():
    for name, file in pandasDF.items():
        file.isnull().sum().plot(kind='bar')
        plt.title(f"NA values in {name} dataset")
        plt.xlabel("Columns")
        plt.ylabel("Number of NA values")
        plt.savefig(f"/img/NA_values_{name}.png")



def main():
    convert_to_pandas()
    na_to_nan()


if __name__ == "__main__":
    main()



# pandasDF.replace("NA", pd.np.nan, inplace=True)
# pandasDF.isnull().sum().plot(kind='bar')
# plt.title("NA values in the dataset")
# plt.xlabel("Columns")
# plt.ylabel("Number of NA values")

# plt.savefig("NA_values.png")

