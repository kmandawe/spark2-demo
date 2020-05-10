from datetime import datetime

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

complex_data = sc.parallelize([Row(col_float=1.44, col_integer=10, col_string="John")])
complex_data_df = complex_data.toDF()
complex_data_df.show()

complex_data = sc.parallelize(
    [Row(col_float=1.44, col_integer=10, col_string="John", col_boolean=True, col_list=[1, 2, 3])])
complex_data_df = complex_data.toDF()
complex_data_df.show()

complex_data = sc.parallelize([Row(col_list=[1, 2, 3], col_dict={"k1": 0}, col_row=Row(a=10, b=20, c=30),
                                   col_time=datetime(2014, 8, 1, 14, 1, 5)),
                               Row(col_list=[1, 2, 3, 4, 5], col_dict={"k1": 0, "k2": 1}, col_row=Row(a=40, b=50, c=60),
                                   col_time=datetime(2014, 8, 2, 14, 1, 6)),
                               Row(col_list=[1, 2, 3, 4, 5, 6, 7], col_dict={"k1": 0, "k2": 1, "k3": 2},
                                   col_row=Row(a=70, b=80, c=90),
                                   col_time=datetime(2014, 8, 3, 14, 1, 7))
                               ])

complex_data_df = complex_data.toDF()
complex_data_df.show()
