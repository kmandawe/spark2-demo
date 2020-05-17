from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Different Join Operatons").getOrCreate()

valuesA = [('John', 100000), ('James', 150000), ('Emily', 65000), ('Nina', 200000)]
tableA = spark.createDataFrame(valuesA, ['name', 'salary'])
tableA.show()

valuesB = [('James', 2), ('Emily', 3), ('Darth Vader', 5), ('Princess Leia', 6)]
tableB = spark.createDataFrame(valuesB, ['name', 'employee_id'])
tableB.show()

inner_join = tableA.join(tableB, tableA.name == tableB.name)
inner_join.show()

left_join = tableA.join(tableB, tableA.name == tableB.name, how='left')
left_join.show()

right_join = tableA.join(tableB, tableA.name == tableB.name, how='right')
right_join.show()

full_outer_join = tableA.join(tableB, tableA.name == tableB.name, how='full')
full_outer_join.show()



