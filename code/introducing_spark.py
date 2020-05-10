from pyspark.shell import spark
from pyspark.sql.types import Row

sc = spark.sparkContext
print(sc)
simple_data = sc.parallelize([1, "Alice", 50])
print(simple_data)
print(simple_data.count())
print(simple_data.first())
print(simple_data.take(2))
print(simple_data.collect())

# df = (simple_data.toDF())
# print(df)

records = sc.parallelize([[1, "Alice", 50], [2, "Bob", 80]])
print(records)
print(records.collect())
print(records.count())
print(records.first())
df = records.toDF()
print(df)
df.show()

data = sc.parallelize([Row(id=1, name="Alice", score=50)])
print(data)
print(data.count)
print(data.collect())
df = data.toDF()
df.show()

data = sc.parallelize([Row(id=1, name="Alice", score=50),
                       Row(id=2, name="Bob", score=80),
                       Row(id=3, name="Charlee", score=75)])

df = data.toDF()
df.show()
