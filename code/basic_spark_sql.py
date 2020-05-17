from pyspark.shell import sqlContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analyzing airline data").getOrCreate()
sc = spark.sparkContext

from pyspark.sql.types import Row
from datetime import datetime

record = sc.parallelize([Row(id=1,
                             name="Jill",
                             active=True,
                             clubs=['chess', 'hockey'],
                             subjects={"math": 80, "english": 56},
                             enrolled=datetime(2014, 8, 1, 14, 1, 5)),
                         Row(id=2,
                             name="George",
                             active=False,
                             clubs=['chess', 'soccer'],
                             subjects={"math": 60, "english": 96},
                             enrolled=datetime(2015, 3, 21, 8, 2, 5)),
                         ])

record_df = record.toDF()
record_df.show()

record_df.createOrReplaceTempView("records")

all_records_df = sqlContext.sql('SELECT * FROM records')
all_records_df.show()

sqlContext.sql('SELECT id, clubs[1], subjects["english"] FROM records').show()

sqlContext.sql('SELECT id, NOT active from records').show()

sqlContext.sql('SELECT * FROM records where active').show()

sqlContext.sql('SELECT * FROM records where subjects["english"] > 90').show()

record_df.createGlobalTempView("global_records")

sqlContext.sql('SELECT * FROM global_temp.global_records').show()

