# Import Libraries
from pyspark import SparkConf, SparkContext  #from pacakge import classes/function
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import pickle
from pyspark.ml.linalg import DenseVector, SparseVector, Vectors, VectorUDT
from pyspark.sql.functions import col, split
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import Normalizer
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, FloatType
import numpy as np
import csv
from pyspark.sql import Row


from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql import SQLContext, Row
from pyspark.sql.session import SparkSession
from pyspark.ml.linalg import DenseVector, SparseVector
from pyspark.ml.feature import Normalizer
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.ml.feature import VectorSlicer
from pyspark.ml.linalg import Vectors
import pandas as pd

from pyspark import SparkConf, SparkContext  #from pacakge import classes/function
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql import SQLContext, Row
from pyspark.sql.session import SparkSession
from pyspark.ml.linalg import DenseVector
from pyspark.ml.feature import Normalizer
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix

import numpy as np
from pyspark.ml.linalg import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkContext.getOrCreate()
#sqlContext = SQLContext(spark)
sparkSess = SparkSession(spark)


def cos_sim(a,b):
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

# rddTrain = spark.textFile("/home/leena/dev/darwin_api/server/scripts/google_images/new_train.csv")
# rddTrain = rddTrain.mapPartitions(lambda x: csv.reader(x))
# Result_train = rddTrain.map(lambda x: (x[0], x[1], x[2],DenseVector((x[3]).split(',')))).toDF(["index", "url", "productId", "features"])

rddTrain = spark.textFile("/home/leena/dev/visualsearch/new_train.csv")
rddTrain = rddTrain.mapPartitions(lambda x: csv.reader(x))
Result_train = rddTrain.map(lambda x: (x[0], x[1], x[2],DenseVector((x[3]).split(',')))).toDF(["index", "url", "productId", "features"])

DataFile_test = spark.textFile('new_test.csv')
Result_test = DataFile_test.map(lambda line : line.split("\n")).flatMap(lambda words : (word.split(",") for word in words))\
.zipWithIndex().map(lambda x: (DenseVector(x[0]), x[1])).toDF(["features_test", "index"])
    #
    # print Result_train.show()
    # print Result_test.show()

normalizer = Normalizer(inputCol="features", outputCol="normFeatures_train", p=2.0)
l1NormData = normalizer.transform(Result_train);


normalizer = Normalizer(inputCol="features_test", outputCol="normFeatures_test", p=2.0)
l1NormData1 = normalizer.transform(Result_test);

print l1NormData.show();
print l1NormData1.show();

Train_DF = l1NormData.createOrReplaceTempView("Result_train1")
# sqlDF_train = sqlContext.sql("select * from Result_train1 limit 1").show()


Test_DF = l1NormData1.createOrReplaceTempView("Result_test1")
# sqlDF_test = sqlContext.sql("select * from Result_test1 limit 1").show()

# # new = pd.concat([pd.l1NormData, pd.l1NormData1], axis=1)
# print l1NormData.show()
# # print l1NormData1.show()
# val sparkConf = new SparkConf().setAppName("Test")
# sparkConf.set("spark.sql.crossJoin.enabled", "true")
# spark.sql.crossJoin.enabled = true
join = sparkSess.sql("select a.index,a.url,a.productId, a.normFeatures_train,b.normFeatures_test from Result_train1 a cross Join Result_test1 b")
#
# print join.show()
df = join.withColumn("coSim", udf(cos_sim, FloatType())(join.normFeatures_train,join.normFeatures_test))
#
# print df
print df.sort(df.coSim.desc()).show()

#Result_test = rdd1.map(lambda x: (x[0], x[1], x[2],DenseVector((x[3]).split(',')))).toDF(["index", "url", "productId", "features"])
# Result_train..withColumn("features", as_vector("features"))


# Result_train = DataFile.map(lambda line : line.split("\n")) \
# .map(lambda x: (x[0],x[1],x[2],x[3])).toDF(["index","url","product_id","features"])
# #.flatMap(lambda words : (word.split(",") for word in words))\
# , x[1],x[2],DenseVector(x[3]))).toDF(["index","url","product_id","features"]).show()
#
# print Result_train.show()
#


# rdd = spark.textFile("/home/leena/dev/darwin_api/server/scripts/google_images/new_train.csv")
# rdd1 = rdd.mapPartitions(lambda x: csv.reader(x))
# Result = rdd1.map(lambda x: (x[0], x[1], x[2],x[3])).toDF(["index", "url", "productId", "features"])
# print Result.printSchema()
#
# with open("/home/leena/dev/darwin_api/server/scripts/google_images/new_train.csv", 'r') as csv_file:
#     csv_reader = csv.reader(csv_file)
#     next(csv_reader)
#     with open("/home/leena/dev/darwin_api/server/scripts/google_images/features.csv", 'w') as new_file:
#         csv_writer = csv.writer(new_file)
#         for line in csv_reader:
#             #print(line[3])
#             csv_writer.writerow(line[3])

#print rdd1.collect();
#Vectors.dense(str_dense_vec.drop(1).dropRight(1).split(',').map(_.toDouble))
#print Result.show();
# df_with_vectors = Result_train.select( Result_train["features"].cast(VectorUDT()))
# print df_with_vectors
#
# assembler = VectorAssembler(inputCols=["features"], outputCol="features")
# df_fail = assembler.transform(Result_train)
#
# # print Result_train.printSchema()
# from pyspark.sql.functions import to_json, from_json, col, struct, lit
# from pyspark.sql.types import StructType, StructField
# from pyspark.ml.linalg import VectorUDT
#
# customSchema =[
#     StructField("Index", StringType(),True),
#     StructField("URL", StringType(), True),
#     StructField("Product_ID", StringType(), True),
#     StructField("Features", DoubleType(), True)]
#
# schema1 = StructType(customSchema)
#
# train = sqlContext.read.format("com.databricks.spark.csv").option("delimiter",",").schema(schema1).load("/home/leena/dev/darwin_api/server/scripts/google_images/new_train.csv").toDF("Index","URL","Product_ID","Feature")
#
# # print train.printSchema()
#
# #
# # Result_test = DataFile_test.map(lambda line : line.split("\n")).flatMap(lambda words : (word.split(",") for word in words))\
# # .zipWithIndex().map(lambda x: (DenseVector(x[0]), x[1])).toDF(["features", "index"])
# #
# #
# index_rdd = train.rdd.map(lambda row:row[0])
# feauture_rdd = Result_train.rdd.map(lambda row:row[3])
# # new_df = index_rdd.zip(feauture_rdd.map(lambda x:Vectors.dense(x))).toDF(schema=['id','new_features'])
# #
#
# Result = feauture_rdd.map(lambda line : line.split("\n")).flatMap(lambda words : (word.split(",") for word in words))\
# .map(lambda x: (DenseVector(x)))
# print new_df.show();
# print Result.collect();
#
# normalizer = Normalizer(inputCol="features", outputCol="normFeatures_train", p=2.0)
# l1NormData = normalizer.transform(new_dff);

# print llNormData.show();

# with_vec = Result_train.withColumn("vector", as_vector("features"))
# with_vec.show
#
# json_vec = to_json(struct(struct(
#     lit(1).alias("type"),  # type 1 is dense, type 0 is sparse
#     col("temperatures").alias("values")
# ).alias("v")))
#
# schema = StructType([StructField("v", VectorUDT())])
#
# with_parsed_vector = df.withColumn(
#     "parsed_vector", from_json(json_vec, schema).getItem("v")
# )
#
# with_parsed_vector.show()
#
# df_with_vectors = Result_train.rdd.map(lambda row: Row(
#     features=Vectors.dense(row["features"])
# )).toDF()
# Result_train.withColumn(
#     "features",
#     split(col("features"), ",\s*").cast("array<int>").alias("features11")
# )
#
# Result_train.withColumn(
#     "features",
#     split(col("features"), ",\s*").cast(ArrayType(IntegerType())).alias("ev")
# )
# print Result_train.printSchema()


# Vectors.dense(x[3].drop(1).dropRight(1).split(',').map(_.toDouble))
# Result_test = x[3].flatMap(lambda words : (word.split(",") for word in words))map(lambda x: (DenseVector(x[0]), x[1])).toDF(["features", "index"])
# assembler = VectorAssembler(
#   inputCols=["features"], outputCol="features"
# )
# assembled = assembler.transform(Result_train)
# print assembled.show()


# normalizer = Normalizer(inputCol="features", outputCol="normFeatures_train", p=2.0)
# l1NormData = normalizer.transform(Result_train);
# testNormData = normalizer_test.transform(Test)
# #
#
#print l1NormData.show()
# print testNormData.show()
