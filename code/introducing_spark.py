from pyspark.shell import spark
from pyspark.sql.types import Row
from datetime import datetime
sc = spark.sparkContext
print(sc)
simple_data = sc.parallelize([1, "Alice", 50])
print(simple_data)
print(simple_data.count())
print(simple_data.first())
print(simple_data.take(2))
print(simple_data.collect())

df = spark.createDataFrame(simple_data)
print(df)

