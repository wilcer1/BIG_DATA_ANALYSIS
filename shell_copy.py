import cmd
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import sys
import os
import matplotlib.ticker as ticker
import pymongo
import dotenv

# Connect to the MongoDB Atlas cluster
dotenv.load_dotenv()
mongo_uri = os.getenv("MONGO_URI")
client = pymongo.MongoClient(mongo_uri)
db = client["BigDataAnalysis"]

# Define the users collection
users = db["users"]



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
            self.df[str(i)] = self.spark.read.format("csv").option("header", "true").option("nullValue", "NA").load("hdfs://localhost:9000/sample2/dataset/"+str(i)+".csv")
            print(i, " file loaded")
        self.airportdf = self.spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/sample2/dataset/airports.csv")
        print("All files loaded")

        cols_to_drop = ["Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier","TailNum", "ActualElapsedTime", "CRSElapsedTime", "AirTime", "TaxiIn", "TaxiOut", "CancellationCode", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]
        for name, file in self.df.items():
            self.df[name] = file.drop(*cols_to_drop)
            print(name, " file cleaned")
            print(self.df[name].columns)
        
    
        self.df_backup = self.df
    
    
    def do_plot2_null_values(self, line):
        """Plot null values in the dataset and save them to img folder"""
        for name, file in self.df.items():
            null_counts = file.select(*(F.sum(F.isnan(F.col(c)).cast("int")).alias(c) for c in file.columns)).toPandas().iloc[0]
            plt.figure(figsize=(10, 15))
            plt.xticks(rotation=90)
            plt.bar(null_counts.index, null_counts.values)
            print("name: ", name)
            plt.xlabel("Columns")
            plt.ylabel("Number of null values")
            plt.title("Distribution of null values across columns")
            
            plt.savefig(f"img\\na\\NA_values_{name}.png")
            plt.clf()

    def do_plot_arr_delay(self, line):
        """Plot the average arr delay over time"""
        
        
        plt.figure(figsize=(15, 10))
        avg_delays = {}
        file_names = []
        max_avg = 0
        i = 0
        for name, file in self.df.items():
            self.df[name].na.drop(subset=["ArrDelay"])
            print("name: ", name, f"{i}/{len(self.df)}")
            i+=1
            
            # get the list of delays
            delays = file.select("ArrDelay")
            
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
        
        self.df = self.df_backup
        # Save the plot to a new file
        plt.savefig(f"img\\delay\\arr_delay_comparison.png")
        plt.clf()
    
    def do_plot_cancelled(self, line):
        """Plot the average number of cancelled flights over time"""
        
        plt.figure(figsize=(20, 20))
        cancelled = {}
        file_names = []
        max_cancelled = 0
        i = 0
        for name, file in self.df.items():
            self.df[name].na.drop(subset=["Cancelled"])
            print("name: ", name, f"{i}/{len(self.df)}")
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
        
        self.df = self.df_backup
        # Save the plot to a new file
        plt.savefig(f"img\\cancelled\\cancelled_comparison.png")
        plt.clf()
    
    def do_plot_average_distance(self, line):
        """Plot the average distance over time"""
        plt.figure(figsize=(15, 10))
        avg_distance_dict = {}
        file_names = []
        max_avg = 0
        i = 0
        for name, file in self.df.items():
            print("name: ", name, f"{i}/{len(self.df)}")
            self.df[name].na.drop(subset=["Distance"])
            i+=1
            
            # get the list of Distances
            distance = file.select("Distance")
            # get the average
            avg_distance = distance.agg(F.avg("Distance")) #DataFrame[avg(Distance): double]
            
            
            avg_distance = avg_distance.first()[0]
            
            max_avg = max(max_avg, avg_distance)
            avg_distance_dict[name] = avg_distance
            file_names.append(name)
        
        plt.plot(file_names, avg_distance_dict.values())
        for name, value in avg_distance_dict.items():
            avg_distance_dict[name] = round(value)
            
        plt.xlabel("Year")
        plt.ylabel("Distance (miles, rounded)")
        plt.title("Distance over Time")
        # plt.legend()
        plt.ylim(round(min(avg_distance_dict.values())), round(max(avg_distance_dict.values()))+1)
        print(f"{range(round(min(avg_distance_dict.values())), round(max(avg_distance_dict.values()))+1), 10}")
        # Set the y-axis ticks
        plt.yticks(range(round(min(avg_distance_dict.values())), round(max(avg_distance_dict.values()))+1, 10))
        print(round(max_avg))
        plt.xticks(range(0, len(file_names), 1))

        # Set the tick labels to the file names using a FixedFormatter
        formatter = ticker.FixedFormatter(file_names)
        plt.gca().xaxis.set_major_formatter(formatter)
        
        self.df = self.df_backup
        # Save the plot to a new file
        plt.savefig(f"img\\distance\\distance_comparison.png")
        plt.clf()

    def do_plot_diverted(self, line):
        """Plot number of diverted flights each year"""
        plt.figure(figsize=(15, 10))
        diverted_dict = {}
        file_names = []
        
        i = 0
        for name, file in self.df.items():
            print("name: ", name, f"{i}/{len(self.df)}")
            self.df[name].na.drop(subset=["Diverted"])
            i+=1
            
            
            diverted_flights = file.select("Diverted")
            diverted_dict[name] = diverted_flights.filter(diverted_flights["Diverted"] == 1).count()
            file_names.append(name)
            
            
            
        
        plt.plot(file_names, diverted_dict.values())
        plt.xlabel("Year")
        plt.ylabel("Total num of Diverted Flights")
        plt.title("Diverted Flights over Time")
        plt.ylim(0, round(max(diverted_dict.values()))+1)

        # Set the y-axis ticks
        plt.yticks(range(0, round(max(diverted_dict.values()))+1, round(max(diverted_dict.values())/10)))
        plt.xticks(range(0, len(file_names), 1))

        # Set the tick labels to the file names using a FixedFormatter
        formatter = ticker.FixedFormatter(file_names)
        plt.gca().xaxis.set_major_formatter(formatter)
        
        self.df = self.df_backup
        # Save the plot to a new file
        plt.savefig(f"img\\diverted\\diverted_comparison.png")
        plt.clf()


    def do_plot_num_flights(self, line):
        """Plots the number of flights per year"""
        plt.figure(figsize=(15, 10))
        num_flights_dict = {}
        file_names = []
        i = 0
        for name, file in self.df.items():
            print("name: ", name, f"{i}/{len(self.df)}")
            self.df[name].na.drop(subset=["Year"])
            i+=1
            
            
            year_col = file.select("Year")
            
            count = year_col.count()
            
            
            num_flights_dict[name] = count
            file_names.append(name)
        
        plt.plot(file_names, num_flights_dict.values())
        plt.xlabel("Year")
        plt.ylabel("Number of Flights")
        plt.title("Number of Flights over Time")
        # plt.legend()
        # set the y-axis ticks to be every 60 minutes and the max value to be the max delay
        plt.ylim(round(min(num_flights_dict.values())), round(max(num_flights_dict.values()))+1)

        # Set the y-axis ticks
        plt.yticks(range(round(min(num_flights_dict.values())), round(max(num_flights_dict.values()))+1, round(max(num_flights_dict.values())/10)))
        plt.xticks(range(0, len(file_names), 1))

        # Set the tick labels to the file names using a FixedFormatter
        formatter = ticker.FixedFormatter(file_names)
        plt.gca().xaxis.set_major_formatter(formatter)
        print("max", max(num_flights_dict.values()))
        
        self.df = self.df_backup
        # Save the plot to a new file
        plt.savefig(f"img\\num_flights\\num_flights_comparison.png")
        plt.clf()  

    def do_plot_diverted_vs_total(self, line):
        """Plot (number of divirted flights divided by total flights)  * 100 by year"""
        plt.figure(figsize=(15, 10))
        num_flights_diverted_divided = {}
        file_names = []
        i = 0
        for name, file in self.df.items():
            print("name: ", name, f"{i}/{len(self.df)}")
            i+=1
            
            
            year_col = file.select("Year")
            
            num_flights = year_col.count()
            diverted_flights = file.select("Diverted")
            diverted_num = diverted_flights.filter(diverted_flights["Diverted"] == 1).count()
            num_flights_diverted_divided[name] = (diverted_num / num_flights ) * 100
            print(num_flights_diverted_divided[name])
            
            file_names.append(name)
        plt.plot(file_names, num_flights_diverted_divided.values())
        plt.xlabel("Year")
        plt.ylabel("(Number of Diverted Flights / Number of Flights) * 100")
        plt.title("Number of Diverted Flights / Number of Flights over Time (percentage)")
        # plt.legend()
        
        plt.ylim(0, round((max(num_flights_diverted_divided.values())) + 1))

        # Set the y-axis ticks
        #plt.yticks(range(round(min(num_flights_diverted_divided.values())), round(max(num_flights_diverted_divided.values()))+1, 0.1))
        plt.xticks(range(0, len(file_names), 1))

        # Set the tick labels to the file names using a FixedFormatter
        formatter = ticker.FixedFormatter(file_names)
        plt.gca().xaxis.set_major_formatter(formatter)
       
        
        
        # Save the plot to a new file
        plt.savefig(f"img\\diverted\\num_flights_dividedby_diverted.png")
        plt.clf()  
    
    def do_plot_cancelled_vs_num_flights(self, line):
        """Plot (number of flights divided by total cancelled flights) * 100 by year"""
        plt.figure(figsize=(15, 10))
        file_names = []
        num_flights_cancelled_divided = {}
        i = 0
        for name, file in self.df.items():
            print("name: ", name, f"{i}/{len(self.df)}")
            i+=1
            
            
            year_col = file.select("Year")
            
            num_flights = year_col.count()
            cancelled_flights = file.select("Cancelled")
            cancelled_num = cancelled_flights.filter(cancelled_flights["Cancelled"] == 1).count()
            num_flights_cancelled_divided[name] = (cancelled_num / num_flights) * 100 
            print(f"cancelled_num: {cancelled_num} num_flights: {num_flights}")
            print(num_flights_cancelled_divided[name])
            
            file_names.append(name)
        plt.plot(file_names, num_flights_cancelled_divided.values())
        plt.xlabel("Year")
        plt.ylabel("(Number of cancelled flights / Number of Flights) * 100 ")
        plt.title("Number of cancelled flights / Number of Flights over Time (percentage)")
        # plt.legend()
       
        plt.ylim(0,round(max(num_flights_cancelled_divided.values())) + 1)

        # Set the y-axis ticks
        #plt.yticks(range(round(min(num_flights_cancelled_divided.values())), round(max(num_flights_cancelled_divided.values()))+1, round(max(num_flights_cancelled_divided.values())/10)))
        plt.xticks(range(0, len(file_names), 1))

        # Set the tick labels to the file names using a FixedFormatter
        formatter = ticker.FixedFormatter(file_names)
        plt.gca().xaxis.set_major_formatter(formatter)
        
        
        
        # Save the plot to a new file
        plt.savefig(f"img\\interactions\\cancelled_vs_num_flights.png")
        plt.clf()  

    def do_plot_origin(self, line):
        """Plot most common origin airport total"""
        plt.figure(figsize=(15, 10))
        file_names = []
        most_common_airports_dict = {}
        i = 0
        for name, file in self.df.items():
            self.df[name].na.drop(subset=["origin"])
            print("name: ", name, f"{i}/{len(self.df)}")
            i+=1
            
            
            origin = file.select("Origin")
            #select most common origin
            grouped_data = origin.groupBy("Origin")
            counts = grouped_data.agg(F.count("Origin").alias("count"))
            # most_common = counts.sort(counts['count'].desc())
            # most_common_value = most_common.first() # iata code
            # most_common_count_value = most_common['count'] # count of said iata code
            
            airport_info = self.airportdf.select("iata", "state")
            joined = counts.join(airport_info, counts["Origin"] == airport_info["iata"], "inner")
            joined = joined.select("state", "count")
            join_dict = joined.collect()
            
            for row in join_dict:
                state = row["state"]
                count = row["count"]
                if state in most_common_airports_dict:
                    most_common_airports_dict[state] += count
                else:
                    most_common_airports_dict[state] = count
            

            
            
            
            
        
        plt.bar(most_common_airports_dict.keys(), most_common_airports_dict.values())
        # plt.gca.xaxis.labelpad = 20
        
        plt.xlabel("State")
        plt.ylabel("Flights departed count")
        plt.title("Most Common Origin States (Total)")

        

        # Set the y-axis ticks
        plt.yticks(range(round(min(most_common_airports_dict.values())), round(max(most_common_airports_dict.values()))+1, round(max(most_common_airports_dict.values())/10)))
        plt.xticks(range(0, len(most_common_airports_dict.keys()), 1), rotation=70)
        
        
       
        
        self.df = self.df_backup
        # Save the plot to a new file
        plt.savefig(f"img\\airports\\origin_state.png")
        plt.clf()  


    def do_plot_arr_delay_state(self, line):
        """Plot the average arr delay per state"""
        plt.figure(figsize=(15, 10))
        total_delays = {}
        most_common_airports_dict = {}
        total_divided = {}
        file_names = []
        max_avg = 0
        i = 0
        for name, file in self.df.items():
            print("name: ", name, f"{i}/{len(self.df)}")
            self.df[name].na.drop(subset=["origin"])
            self.df[name].na.drop(subset=["ArrDelay"])
            i += 1
            
            origin = file.select("Origin")
            #select most common origin
            grouped_data = origin.groupBy("Origin")
            counts = grouped_data.agg(F.count("Origin").alias("count"))
            
            
            airport_info = self.airportdf.select("iata", "state")
            joined = counts.join(airport_info, counts["Origin"] == airport_info["iata"], "inner")
            joined = joined.select("state", "count")
            join_dict = joined.collect()
            
            for row in join_dict:
                state = row["state"]
                count = row["count"]
                if state in most_common_airports_dict:
                    most_common_airports_dict[state] += count
                else:
                    most_common_airports_dict[state] = count
            # get the list of delays
            delays = file.select("ArrDelay")
            # filter out the NA values
            total_delay = delays.agg(F.sum("ArrDelay"))
            # get the average delay of the file
            total_delay = total_delay.first()[0]
            max_total = max(max_avg, total_delay)
            total_delays[name] = total_delay

            for k in most_common_airports_dict:
                if k not in total_divided:
                    total_divided[k] = most_common_airports_dict[k] / total_delays[name]
                else:
                    total_divided[k] += most_common_airports_dict[k] / total_delays[name]
            file_names.append(name)
        print(total_divided)
        
        plt.bar(total_divided.keys(), total_divided.values())
        plt.xlabel("State")
        plt.ylabel("Flights Origin count / Arr Delay (minutes)")
        plt.title("Flights Origin count / Arr Delay (minutes) per state")
       
        plt.ylim(0, round(max(total_divided.values()))+1)

        # Set the y-axis ticks
        plt.yticks(range(0, round(max(total_divided.values()))+1))
        
        plt.xticks(range(0, len(total_divided.keys()), 1), rotation=70)

        # Set the tick labels to the file names using a FixedFormatter
        # formatter = ticker.FixedFormatter(file_names)
        # plt.gca().xaxis.set_major_formatter(formatter)
        
        self.df = self.df_backup
        # Save the plot to a new file
        plt.savefig(f"img\\airports\\arr_delay_state_comparison.png")
        plt.clf()
        
    def do_plot_dest(self, line):
        """Plot most common origin airport total"""
        plt.figure(figsize=(15, 10))
        file_names = []
        most_common_airports_dict = {}
        i = 0
        
        for name, file in self.df.items():
            print("name: ", name, f"{i}/{len(self.df)}")
            self.df[name].na.drop(subset=["Dest"])
            i+=1
            
            
            origin = file.select("Dest")
            #select most common origin
            grouped_data = origin.groupBy("Dest")
            counts = grouped_data.agg(F.count("Dest").alias("count"))
            # most_common = counts.sort(counts['count'].desc())
            # most_common_value = most_common.first() # iata code
            # most_common_count_value = most_common['count'] # count of said iata code
            
            airport_info = self.airportdf.select("iata", "state")
            joined = counts.join(airport_info, counts["Dest"] == airport_info["iata"], "inner")
            joined = joined.select("state", "count")
            join_dict = joined.collect()
            
            for row in join_dict:
                state = row["state"]
                count = row["count"]
                if state in most_common_airports_dict:
                    most_common_airports_dict[state] += count
                else:
                    most_common_airports_dict[state] = count
            

            
            
            
            
        
        plt.bar(most_common_airports_dict.keys(), most_common_airports_dict.values())
        # plt.gca.xaxis.labelpad = 20
        
        plt.xlabel("State")
        plt.ylabel("Flights departed count")
        plt.title("Most Common Destination States (Total)")

        

        # Set the y-axis ticks
        plt.yticks(range(round(min(most_common_airports_dict.values())), round(max(most_common_airports_dict.values()))+1, round(max(most_common_airports_dict.values())/10)))
        plt.xticks(range(0, len(most_common_airports_dict.keys()), 1), rotation=70)
        
        
       
        
        self.df = self.df_backup
        # Save the plot to a new file
        plt.savefig(f"img\\airports\\dest_state.png")
        plt.clf()  


    def do_drop_null(self, line):
        """Drop all rows with null values"""
        for key, val in self.df.items():
            val = val.dropna()
            self.df[key] = val
            print(f"Dropped null values in {key}")
        
    def do_print_column_head(self, line1):
        """Print the first 20 rows of a column: <Year> <Column>"""
        print(self.df["1987"].select(line1).show(5000))

    def do_show_columns(self, line):
        """Show the columns of the dataset"""
        print(self.df["1987"].columns)

    def do_print_null_count(self, line):
        """Print the number of null values / total values in a column: <Year> <Column>"""
        for name, year in self.df.items():
            # get row count of column
            row_count = year.count()
            
            print(f"{name}: {year.filter(year[line].isNull()).count()} / {row_count}")
    
    def do_plot_null_values(self, line):
        """Plot the number of null values in each column for each year"""
        for name, file in self.df.items():
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

    def do_test(self, line):
        
        null_counts = self.df["1987"].select(*(F.sum(F.isnan(F.col(c)).cast("int")).alias(c) for c in self.df["1987"].columns))
        collected = null_counts.collect()
        
        for i in collected:
            for key, val in i.asDict().items():
                print(f"{key}: {val}")

    def do_EOF(self, line):
        """Exit the shell"""
        return True
    
# Define a function to handle the login process
def login():
    # Ask the user for their username and password
    username = input("Username: ")
    password = input("Password: ")

    # Try to find the user in the users collection
    user = users.find_one({"username": username})

    # If the user exists and the password is correct, log them in
    if user and user["password"] == password:
        print("Login successful!")
        return True
    else:
        print("Login failed.")
        return False

def main():

    # loggedIn = False

    # while not loggedIn:
    #     if login():
    #         loggedIn = True
    #         print("Welcome to the secret area!")
    #         shell = Shell()
    #         shell.start_loop()
    #     else:
    #         print("Please try again.")

    print("Welcome to the secret area!")
    print("Starting hadoop...")
    shell = Shell()
    shell.start_loop()


# Call the login function to start the login process
if __name__ == "__main__":
    main()
