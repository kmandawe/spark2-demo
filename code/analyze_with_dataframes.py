from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analyzing London crime data").getOrCreate()

data = spark.read.format("csv").option("header", "true").load("../datasets/london_crime_by_lsoa.csv")

data.printSchema()

# print(data.count())
#
# data.limit(5).show()
#
# data.dropna()

data = data.drop("lsoa_code")
# data.show(5)
#
# total_boroughs = data.select('borough').distinct()
# total_boroughs.show()
# print(total_boroughs.count())
#
# hackney_data = data.filter(data['borough'] == "Hackney")
# hackney_data.show(5)
#
# data_2015_2016 = data.filter(data['year'].isin(["2015", "2016"]))
# data_2015_2016.sample(fraction=0.1).show()
#
# data_2014_onwards = data.filter(data['year'] >= 2014)
# data_2014_onwards.sample(fraction=0.1).show()

borough_crime_count = data.groupby('borough').count()
borough_crime_count.show(5)

# borough_conviction_sum = data.groupby('borough').agg({"value":"sum"})
# borough_conviction_sum.show(5)

borough_conviction_sum = data.groupby('borough').agg({"value": "sum"}).withColumnRenamed("sum(value)", "convictions")
borough_conviction_sum.show(5)

total_borough_convictions = borough_conviction_sum.agg({"convictions": "sum"})
total_borough_convictions.show()

total_convictions = total_borough_convictions.collect()[0][0]

import pyspark.sql.functions as func

borough_percentage_contribution = borough_conviction_sum.withColumn("% contribution", func.round(
    borough_conviction_sum.convictions / total_convictions * 100, 2))

borough_percentage_contribution.printSchema()
borough_percentage_contribution.orderBy(borough_percentage_contribution[2].desc()).show(10)

conviction_monthly = data.filter(data['year'] == 2014).groupby('month').agg({"value": "sum"}).withColumnRenamed(
    "sum(value)", "convictions")
conviction_monthly.show()

total_conviction_monthly = conviction_monthly.agg({"convictions": "sum"}).collect()[0][0]

total_conviction_monthly = conviction_monthly.withColumn("percent", func.round(
    conviction_monthly.convictions / total_conviction_monthly * 100, 2))
print(total_conviction_monthly.columns)

total_conviction_monthly.orderBy(total_conviction_monthly.percent.desc()).show()
