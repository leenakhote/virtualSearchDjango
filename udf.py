# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# import math
# from pyspark.sql.functions import udf
# from pyspark import SparkConf, SparkContext
# from pyspark.sql import SQLContext, Row
# from pyspark.sql.session import SparkSession
# from pyspark import SparkConf, SparkContext
# from pyspark import sql
#
#  #from pacakge import classes/function
# # self.sc = SparkContext.getOrCreate()
# # self.sparkSession = SparkSession(self.sc)
# # self.sqlContext = SQLContext(self.sc)
# # conf = SparkConf()
# # sc = SparkContext(conf = conf)
# # sqlContext = sql.SQLContext(sc)
# def distance(origin, destination):
#     lat1, lon1 = origin
#     lat2, lon2 = destination
#     radius = 6371 # km
#     dlat = math.radians(lat2-lat1)
#     dlon = math.radians(lon2-lon1)
#     a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
#     * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
#     c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
#     d = radius * c
#     return d
#
# class EXAMPLE:
#     def __init__(self):
#         conf = SparkConf()
#         sc = SparkContext(conf = conf)
#         self.sqlContext = sql.SQLContext(sc)
#
#
#     def begin(self):
#         df = self.sqlContext.createDataFrame([([101,121],[-121,-212])],["origin", "destination"])
#         df.show()
#         filter_udf = udf(distance,DoubleType())
#         result = df.withColumn("distance",filter_udf(df.origin,df.destination))
#         result.show()
#
# example = EXAMPLE()
# y = example.begin()




from pyspark import SparkConf, SparkContext  #from pacakge import classes/function
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql import SQLContext, Row
from pyspark.sql.session import SparkSession
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.ml.feature import VectorSlicer
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors, VectorUDT
import pickle
from pyspark.ml.linalg import DenseVector, SparseVector, Vectors, VectorUDT
from pyspark.sql.functions import col, split
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import Normalizer
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, FloatType
import numpy as np
import csv
from pyspark.sql import Row
from pyspark.ml.linalg import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
import requests
import json
import pandas
import shlex, subprocess
import boto3
import os
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def cosineSimilarity(a,b):
    print "udf"
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

def trainData(imgUrl,sc,sparkSession,sqlContext):

    """Init the recommendation engine given a Spark context and a dataset path
    """
    # sc = SparkContext.getOrCreate()
    # sparkSession = SparkSession(sc)
    # sqlContext = SQLContext(sc)

    bucket = "darwin-vs"
    file_name = "train_no.csv"

    s3 = boto3.client(
        's3',
        aws_access_key_id = 'AKIAJ6IIHKANYPNE5X6A',
        aws_secret_access_key= '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'
    )
    obj = s3.get_object(Bucket= bucket, Key= file_name)
    data = obj['Body'].read()

    logger.info("Starting up the Recommendation Engine: ")

    trainNormalizeFeatures = formatFeaturesDF(data,"TrainNormFeatures",sc,sparkSession,sqlContext)
    print trainNormalizeFeatures.show()
    result = getVisualSearch(imgUrl, trainNormalizeFeatures,sc,sparkSession,sqlContext)
    print result
    return result

def formatFeaturesDF(featuresRawRDD, outputColName,sc,sparkSession,sqlContext):
    # sc = SparkContext.getOrCreate()
    # sparkSession = SparkSession(sc)
    # sqlContext = SQLContext(sc)

    x = featuresRawRDD.strip();
    dataFrameFeatures = sc.parallelize([x])

    dataframe_features1 = dataFrameFeatures.map(lambda line : line.split("\r\n"))\
    .flatMap(lambda words : (word.split(",") for word in words)).map(lambda x : [elem.strip('"') for elem in x])\
    .map(lambda x: (x[0], x[1], x[2],DenseVector(x[3:]))).toDF(["index", "url", "productId", "features"])

    trainNormalizeFeatures = getNormalizer(dataframe_features1,outputColName)
    return trainNormalizeFeatures


def getNormalizer(dataFrameFeatures,outputColName):

    print ("inside normalizer")
    print (dataFrameFeatures)
    #define Normalizer to get normailize freatures
    normalized = Normalizer(inputCol="features", outputCol=outputColName, p=2.0)
    #Get Normalize feature
    normData = normalized.transform(dataFrameFeatures);
    #print normData.show()
    return normData

def getNormalizerTest(dataFrameFeatures,outputColName):

    print ("inside normalizer test")
    print (dataFrameFeatures)
    #define Normalizer to get normailize freatures
    normalized = Normalizer(inputCol="features", outputCol=outputColName, p=2.0)
    #Get Normalize feature
    normData = normalized.transform(dataFrameFeatures);
    #print normData.show()
    return normData


def getCosineSimilarity(testDataFrameFeatures, trainNormalizeFeatures,sc,sparkSession,sqlContext):
    # sc = SparkContext.getOrCreate()
    # sparkSession = SparkSession(sc)
    # sqlContext = SQLContext(sc)
    print ("get cosine")
    print testDataFrameFeatures.show()
    print trainNormalizeFeatures.show()
    #Create Table for temporary View for Train DAta
    trainDataFrameFeaturesNorm = trainNormalizeFeatures.createOrReplaceTempView("trainDataFrameFeatures")
    print trainDataFrameFeaturesNorm
    #sqlContext.sql("select * from trainDataFrameFeaturesNorm limit 1").show()
    #Create Table for temporary View for TEst DAta
    testDataFrameFeaturesNorm = testDataFrameFeatures.createOrReplaceTempView("testDataFrameFeatures")
    print testDataFrameFeaturesNorm
    #sqlContext.sql("select * from testDataFrameFeaturesNorm limit 1").show()

    join = sparkSession.sql("select a.index,a.url,a.productId, a.TrainNormFeatures,b.TestNormFeatures from trainDataFrameFeatures a cross Join testDataFrameFeatures b")
    join.show()
    df = join.withColumn("coSim", udf(cosineSimilarity, FloatType())(join.TrainNormFeatures,join.TestNormFeatures))
    print df.show()
    resultss = df.sort(df.coSim.desc())
    res = resultss.createOrReplaceTempView("test")
    sqlDF_train = sqlContext.sql("select index,url,productId,cosim from test order by cosim desc limit 10")#.toDF("index","url", "productId", "cosim").coalesce(1).write.mode("overwrite").format("json").save("op.json")
    top_match = sqlDF_train.toPandas().to_dict('records')
    print ("****************************",top_match)
    return top_match
    #print self.top_match

def getVisualSearch(imgUrl, trainNormalizeFeatures,sc,sparkSession,sqlContext):
    # sc = SparkContext.getOrCreate()
    # sparkSession = SparkSession(sc)
    # sqlContext = SQLContext(sc)

    print("inside visual search")
    logger.info("inside visualsearch")
    dataframe_features = getTestData(imgUrl)

    a=[json.dumps(dataframe_features)]
    jsonRDD = sc.parallelize(a)
    df = sparkSession.read.json(jsonRDD)

    to_vector = udf(lambda a: DenseVector(a), VectorUDT())
    data = df.select("imgUrl", to_vector("testDataImgFeatures").alias("features"))

    testNormalizeFeatures = getNormalizerTest(data,"TestNormFeatures")

    finalresult = getCosineSimilarity(testNormalizeFeatures, trainNormalizeFeatures,sc,sparkSession,sqlContext)
    return finalresult


def getTestData(imgUrl):
    print("test data")
    # sc = SparkContext.getOrCreate()
    # sparkSession = SparkSession(sc)
    # sqlContext = SQLContext(sc)

    testDataFeature = requests.post('http://ares.styfi.in:81/visualsearch/extractproductfeature/',
                                  data=json.dumps({'imgUrl': imgUrl})).content

    testDataFeature = json.loads(testDataFeature)
    testData = {}
    testData['imgUrl'] = imgUrl
    testData['testDataImgFeatures'] = (testDataFeature['imgFeatures'])
    #print testData
    return testData


sc = SparkContext.getOrCreate()
sparkSession = SparkSession(sc)
sqlContext = SQLContext(sc)

y = trainData("https://s3-ap-southeast-1.amazonaws.com/styfi-image/cache/catalog/products/17442_1496738885213_0-600x750.jpg")
print y
