from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Inferred and explicit schemas").getOrCreate()
sc = spark.sparkContext

from pyspark.sql.types import Row

lines = sc.textFile("../datasets/students.txt")
print(lines.collect())

parts = lines.map(lambda l: l.split(","))
print(parts.collect())

students = parts.map(lambda p: Row(name=p[0], math=int(p[1]), english=int(p[2]), science=int(p[3])))
print(students.collect())

schemaStudents = spark.createDataFrame(students)
schemaStudents.createOrReplaceTempView("students")
print(schemaStudents.columns)
print(schemaStudents.schema)
spark.sql("SELECT * FROM students").show()

print(parts.collect())

schemaString = "name math english science"

from pyspark.sql.types import StructType, StructField, StringType, LongType

fields = [
    StructField('name', StringType(), True),
    StructField('math', LongType(), True),
    StructField('english', LongType(), True),
    StructField('science', LongType(), True)
]

schema = StructType(fields)

schemaStudents = spark.createDataFrame(parts, schema)
print(schemaStudents.columns)
print(schemaStudents.schema)

spark.sql("SELECT * FROM students").show()


