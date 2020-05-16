from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analyzing soccer players").getOrCreate()

players = spark.read.format("csv").option("header", "true").load("../datasets/player.csv")
players.printSchema()
players.show(5)

player_attributes = spark.read.format("csv").option("header", "true").load("../datasets/Player_Attributes.csv")
player_attributes.printSchema()
print((players.count(), player_attributes.count()))

print(player_attributes.select('player_api_id').distinct().count())

players = players.drop('id', 'player_fifa_api_id')
print(players.columns)

player_attributes = player_attributes.drop(
    'id',
    'player_fifa_api_id',
    'preferred_foot',
    'attacking_work_rate',
    'defensive_work_rate',
    'crossing',
    'jumping',
    'sprint_speed',
    'balance',
    'aggression',
    'short_passing',
    'potential'
)
print(player_attributes.columns)

player_attributes = player_attributes.dropna()
players = players.dropna()
print((players.count(), player_attributes.count()))

from pyspark.sql.functions import udf

year_extract_udf = udf(lambda date: date.split('-')[0])

player_attributes = player_attributes.withColumn("year", year_extract_udf(player_attributes.date))
player_attributes = player_attributes.drop('date')
print(player_attributes.columns)

