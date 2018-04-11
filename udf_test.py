# imports we'll need
import numpy as np
from pyspark.ml.linalg import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext, Row


sc = SparkContext.getOrCreate()
sparkSession = SparkSession(sc)
sqlContext = SQLContext(sc)

# function to generate a random Spark dense vector
def random_dense_vector(length=10):
    return Vectors.dense([float(np.random.random()) for i in xrange(length)])

# create a random static dense vector
static_vector = random_dense_vector()

print static_vector
# create a random DF with dense vectors in column
df = sparkSession.createDataFrame([[random_dense_vector()] for x in xrange(10)], ["myCol"])
df.limit(3).toPandas()

print df.show()
# write our UDF for cosine similarity
def cos_sim(a,b):
    print a,b
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

# apply the UDF to the column
df = df.withColumn("coSim", udf(cos_sim, FloatType())(col("myCol"), array([lit(v) for v in static_vector])))
print "9999999999999",df.show()
res = df.limit(10).toPandas()
print res
