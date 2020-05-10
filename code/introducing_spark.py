from datetime import datetime

from pyspark import SQLContext
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

sqlContext = SQLContext(sc)
print(sqlContext)

df = sqlContext.range(5)
print(df)
df.show()
print(df.count())

data = [("Alice", 50), ("Bob", 80), ("Charlee", 75)]
sqlContext.createDataFrame(data).show()

sqlContext.createDataFrame(data, ['Name', 'Score']).show()

complex_data = [
    (1.0, 10, "Alice", True, [1, 2, 3], {"k1": 0}, Row(a=1, b=2, c=3), datetime(2014, 8, 1, 14, 1, 5)),
    (2.0, 20, "Bob", True, [1, 2, 3, 4, 5], {"k1": 0, "k2": 1}, Row(a=1, b=2, c=3), datetime(2014, 8, 1, 14, 1, 5)),
    (3.0, 30, "Charlee", False, [1, 2, 3, 4, 5, 6], {"k1": 0, "k2": 1, "k3": 2}, Row(a=1, b=2, c=3),
     datetime(2014, 8, 1, 14, 1, 5)),
]

sqlContext.createDataFrame(complex_data).show()

complex_data_df = sqlContext.createDataFrame(complex_data,
                                             ['col_integer',
                                              'col_float',
                                              'col_string',
                                              'col_boolean',
                                              'col_list',
                                              'col_dictionary',
                                              'col_row',
                                              'col_date_time']
                                             )
complex_data_df.show()

data = sc.parallelize([Row(1, "Alice", 50),
                       Row(2, "Bob", 80),
                       Row(3, "Charlee", 75)])

column_names = Row('id', 'name', 'score')
students = data.map(lambda r: column_names(*r))
print(students)
print(students.collect())
students_df = sqlContext.createDataFrame(students)
students_df.show()

print(complex_data_df.first())
print(complex_data_df.take(2))

cell_string = complex_data_df.collect()[0][2]
print(cell_string)

cell_list = complex_data_df.collect()[0][4]
print(cell_list)

cell_list.append(100)
print(cell_list)

complex_data_df.show()

string_dict_rdd = complex_data_df.rdd.map(lambda x: (x.col_string, x.col_dictionary)).collect()
print(string_dict_rdd)

complex_data_df.select('col_string', 'col_list', 'col_date_time').show()

with_boo = complex_data_df.rdd.map(lambda x: (x.col_string + " Boo")).collect()
print(with_boo)

complex_data_df.select('col_integer', 'col_float') \
    .withColumn("col_sum", complex_data_df.col_integer + complex_data_df.col_float).show()

complex_data_df.select('col_boolean').withColumn("col_opposite", complex_data_df.col_boolean == False).show()

complex_data_df.withColumnRenamed("col_dictionary", "col_map").show()

complex_data_df.select(complex_data_df.col_string.alias("Name")).show()

import pandas

df_pandas = complex_data_df.toPandas()
print(df_pandas)

df_spark = sqlContext.createDataFrame(df_pandas)
df_spark.show()



