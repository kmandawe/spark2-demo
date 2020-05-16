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

pa_striker_2016 = pa_2016.groupBy('player_api_id').agg({'finishing': "avg", "shot_power": "avg", "acceleration": "avg"})
pa_striker_2016 = pa_striker_2016.withColumnRenamed("avg(finishing)", "finishing").withColumnRenamed(
    "avg(acceleration)", "acceleration").withColumnRenamed("avg(shot_power)", "shot_power")

weight_finishing = 1
weight_shot_power = 2
weight_acceleration = 1

total_weight = weight_finishing + weight_shot_power + weight_acceleration

strikers = pa_striker_2016.withColumn("striker_grade",
                                      (pa_striker_2016.finishing * weight_finishing +
                                       pa_striker_2016.shot_power * weight_shot_power +
                                       pa_striker_2016.acceleration * weight_acceleration) / total_weight)

strikers = strikers.drop('finishing', 'acceleration', 'shot_power')

strikers = strikers.filter(strikers.striker_grade > 70).sort(strikers.striker_grade.desc())

# striker_details = players.join(strikers, players.player_api_id == strikers.player_api_id)
# print(striker_details.columns)
# print(striker_details.count())
#
# striker_details = players.join(strikers, ['player_api_id'])
# striker_details.show(5)

from pyspark.sql.functions import broadcast

striker_details = players.select("player_api_id", "player_name").join(broadcast(strikers), ['player_api_id'], 'inner')
striker_details = striker_details.sort(striker_details.striker_grade.desc())
striker_details.show(5)
