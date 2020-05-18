from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analyzing airline data").getOrCreate()

from pyspark.sql.types import Row
from datetime import datetime

airlinesPath = "../datasets/airlines.csv"
flightsPath = "../datasets/flights.csv"
airportsPath = "../datasets/airports.csv"

airlines = spark.read.format("csv").option("header", "true").load(airlinesPath)
airlines.createOrReplaceTempView("airlines")

airlines = spark.sql("SELECT * FROM airlines")
print(airlines.columns)
airlines.show(5)

flights = spark.read.format("csv").option("header", "true").load(flightsPath)
flights.createOrReplaceTempView("flights")
print(flights.columns)
flights.show(5)

print(flights.count(), airlines.count())

flights_count = spark.sql("SELECT COUNT(*) FROM flights")
airlines_count = spark.sql("SELECT COUNT(*) FROM airlines")
print(flights_count, airlines_count)
print(flights_count.collect()[0][0], airlines_count.collect()[0][0])

total_distance_df = spark.sql("SELECT distance from flights").agg({"distance": "sum"}).withColumnRenamed(
    "sum(distance)", "total_distance")
total_distance_df.show()

all_delays_2012 = spark.sql(
    "SELECT date, airlines, flight_number, departure_delay FROM flights WHERE departure_delay > 0 "
    "and year(date) = 2012")
all_delays_2012.show()

all_delays_2014 = spark.sql(
    "SELECT date, airlines, flight_number, departure_delay FROM flights WHERE departure_delay > 0 "
    "and year(date) = 2014")
all_delays_2014.show(5)
all_delays_2014.createOrReplaceTempView("all_delays")
all_delays_2014.orderBy(all_delays_2014.departure_delay.desc()).show(5)

delay_count = spark.sql("SELECT COUNT(departure_delay) FROM all_delays")
delay_count.show()

delay_percent = delay_count.collect()[0][0] / flights_count.collect()[0][0] * 100
print(delay_percent)

delay_per_airline = spark.sql("SELECT airlines, departure_delay FROM flights").groupBy("airlines").agg(
    {"departure_delay": "avg"}).withColumnRenamed("avg(departure_delay)", "departure_delay")

delay_per_airline.orderBy(delay_per_airline.departure_delay.desc()).show(5)
delay_per_airline.createOrReplaceTempView("delay_per_airline")

delay_per_airline = spark.sql("SELECT * FROM delay_per_airline ORDER by departure_delay DESC")
delay_per_airline.show(5)

delay_per_airline = spark.sql("SELECT * FROM delay_per_airline JOIN airlines ON airlines.code = "
                              "delay_per_airline.airlines ORDER BY departure_delay DESC")
delay_per_airline.show(5)


