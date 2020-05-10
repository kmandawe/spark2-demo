from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analyzing London crime data").getOrCreate()

data = spark.read.format("csv").option("header", "true").load("../datasets/london_crime_by_lsoa.csv")

data.printSchema()

print(data.count())

data.limit(5).show()

data.dropna()

data = data.drop("lsoa_code")
data.show(5)

total_boroughs = data.select('borough').distinct()
total_boroughs.show()
print(total_boroughs.count())

hackney_data = data.filter(data['borough'] == "Hackney")
hackney_data.show(5)

data_2015_2016 = data.filter(data['year'].isin(["2015", "2016"]))
data_2015_2016.sample(fraction=0.1).show()

data_2014_onwards = data.filter(data['year'] >= 2014)
data_2014_onwards.sample(fraction=0.1).show()
