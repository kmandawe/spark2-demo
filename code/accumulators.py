from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analyzing soccer players").getOrCreate()

players = spark.read.format("csv").option("header", "true").load("../datasets/player.csv")
# players.printSchema()
# players.show(5)

player_attributes = spark.read.format("csv").option("header", "true").load("../datasets/Player_Attributes.csv")
# player_attributes.printSchema()
# print((players.count(), player_attributes.count()))

# print(player_attributes.select('player_api_id').distinct().count())

players = players.drop('id', 'player_fifa_api_id')
# print(players.columns)

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
# print(player_attributes.columns)

player_attributes = player_attributes.dropna()
players = players.dropna()
# print((players.count(), player_attributes.count()))

from pyspark.sql.functions import udf

year_extract_udf = udf(lambda date: date.split('-')[0])

player_attributes = player_attributes.withColumn("year", year_extract_udf(player_attributes.date))
player_attributes = player_attributes.drop('date')
# print(player_attributes.columns)

pa_2016 = player_attributes.filter(player_attributes.year == 2016)

print(players.count(), player_attributes.count())

from pyspark.sql.functions import broadcast

players_heading_acc = player_attributes.select('player_api_id', 'heading_accuracy') \
    .join(broadcast(players), player_attributes.player_api_id == players.player_api_id)

print(players_heading_acc.columns)

short_count = spark.sparkContext.accumulator(0)
medium_low_count = spark.sparkContext.accumulator(0)
medium_high_count = spark.sparkContext.accumulator(0)
tall_count = spark.sparkContext.accumulator(0)


def count_players_by_height(row):
    height = float(row.height)

    if height <= 175:
        short_count.add(1)
    elif 183 >= height > 175:
        medium_low_count.add(1)
    elif 195 >= height > 183:
        medium_high_count.add(1)
    elif height > 195:
        tall_count.add(1)


players_heading_acc.foreach(lambda x: count_players_by_height(x))

all_players = [short_count.value,
               medium_low_count.value,
               medium_high_count.value,
               tall_count.value]

print(all_players)

short_ha_count = spark.sparkContext.accumulator(0)
medium_low_ha_count = spark.sparkContext.accumulator(0)
medium_high_ha_count = spark.sparkContext.accumulator(0)
tall_ha_count = spark.sparkContext.accumulator(0)


def count_players_by_height_and_heading_accuracy(row, threshold_score):
    height = float(row.height)
    ha = float(row.heading_accuracy)

    if ha <= threshold_score:
        return

    if height <= 175:
        short_ha_count.add(1)
    elif 183 >= height > 175:
        medium_low_ha_count.add(1)
    elif 195 >= height > 183:
        medium_high_ha_count.add(1)
    elif height > 195:
        tall_ha_count.add(1)


players_heading_acc.foreach(lambda x: count_players_by_height_and_heading_accuracy(x, 60))
all_players_above_threshold = [short_ha_count.value,
                               medium_low_ha_count.value,
                               medium_high_ha_count.value,
                               tall_ha_count.value]

print(all_players_above_threshold)

percentage_values = [short_ha_count.value / short_count.value * 100,
                     medium_low_ha_count.value / medium_low_count.value * 100,
                     medium_high_ha_count.value / medium_high_count.value * 100,
                     tall_ha_count.value / tall_count.value * 100]

print(percentage_values)
