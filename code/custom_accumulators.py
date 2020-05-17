from pyspark.accumulators import AccumulatorParam
from pyspark.shell import spark

sc = spark.sparkContext


class VectorAccumulatorParam(AccumulatorParam):

    def zero(self, value):
        return [0.0] * len(value)

    def addInPlace(self, value1, value2):
        for i in range(len(value1)):
            value1[i] += value2[i]
        return value1


vector_accum = sc.accumulator([10.0, 20.0, 30.0], VectorAccumulatorParam())

print(vector_accum.value)

vector_accum += [1, 2, 3]

print(vector_accum.value)
