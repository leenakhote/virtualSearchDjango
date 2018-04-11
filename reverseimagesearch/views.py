# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.shortcuts import render
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse,HttpResponse
from reverseimagesearch.visualsearch_engine import VISUALSEARCH
import json
import requests
# Create your views here.



from pyspark import SparkConf, SparkContext  #from pacakge import classes/function
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql import SQLContext, Row
from pyspark.sql.session import SparkSession
from pyspark.ml.linalg import DenseVector, SparseVector
from pyspark.ml.feature import Normalizer
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.ml.feature import VectorSlicer
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, FloatType

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
# import pandas as pd

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
import pickle
import csv
import numpy as np
import os
import requests
import json
import pandas

import numpy as np
from pyspark.ml.linalg import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext, Row
from pyspark import SparkFiles

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import sys
#y = sys.path.append('/tmp/cosine')

# from cosine import cosineSimilarity

#from functions import cosineSimilarity
#import cosine
# def cosineSimilarity(a,b):
#     print a ,b
#     return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
#
# class VISUALSEARCH:
#     """A visual Search APPlication
#     """
#
#     def __init__(self, feature_dataset_path):
#         """Init the recommendation engine given a Spark context and a dataset path
#         """
#
#         logger.info("Starting up the Recommendation Engine: ")
#         conf = SparkConf().setAppName("VISUALSEARCH")
#         self.sc = SparkContext(conf=conf, pyFiles=['/home/leena/dev/visualsearch/reverseimagesearch/views.py'])
#         # self.sc = SparkContext.getOrCreate()
#         self.sparkSession = SparkSession(self.sc)
#         self.sqlContext = SQLContext(self.sc)
#         self.sc.addFile("/tmp/cosine.py")
#         # y = sys.path.insert(0, os.path.abspath('.'))
#         # #y = sys.path.append('/tmp/cosine.py')
#         # print "pathhhhhhhhhhhhhhh", y
#         #print feature_dataset_path
#         # Load Features data for later use
#         logger.info("Loading Features data...")
#         featuresRawRDD = self.sc.textFile(feature_dataset_path)
#         logger.info("Loading Features RDD data...")
#
#         #Map Read CSV file and Map to RDD
#         featuresRdd = featuresRawRDD.mapPartitions(lambda x: csv.reader(x))
#         #featuresss = featuresRdd.map(lambda x : x.encode("ascii", "ignore"))
#         # print featuresRdd.collect()
#         print "000000000000000000000000000000000"
#
#         self.trainNormalizeFeatures = self.formatFeaturesDF(featuresRdd,"TrainNormFeatures")
#         print "1111111111111111111111111111111111111"
#         # print self.trainNormalizeFeatures.show()
#
#     def formatFeaturesDF(self,featuresRawRDD, outputColName):
#
#         # print "00000",featuresRawRDD.collect()
#         print "333333333333333333333333333333333333333333"
#
#         #Convert RDD Data into DataFrame
#         dataframe_features = featuresRawRDD.map(lambda x: (x[0], x[1], x[2],DenseVector((x[3]).split(',')))).toDF(["index", "url", "productId", "features"])
#
#         print "4444444444444444444444444444444444444444"
#         trainNormalizeFeatures = self.getNormalizer(dataframe_features,outputColName)
#         print "555555555555555555555555555555555555555555555"
#         return trainNormalizeFeatures
#
#
#     def getNormalizer(self,dataFrameFeatures,outputColName):
#         #define Normalizer to get normailize freatures
#         normalized = Normalizer(inputCol="features", outputCol=outputColName, p=2.0)
#         #Get Normalize feature
#         normData = normalized.transform(dataFrameFeatures);
#         return normData
#
#     def random_dense_vector(self,length=10):
#         return Vectors.dense([float(np.random.random()) for i in xrange(length)])
#
#
#
#     def getCosineSimilarity(self,testDataFrameFeatures):
#         print "****************************** inside cosine "
#         #Create Table for temporary View for Train DAta
#         trainDataFrameFeaturesNorm = self.trainNormalizeFeatures.createOrReplaceTempView("trainDataFrameFeatures")
#         #Create Table for temporary View for TEst DAta
#         print "88888888888888888888888888888888888888888888888"
#         # print testDataFrameFeatures.show()
#         # testDataFrameFeaturesNorm = testDataFrameFeatures.createOrReplaceTempView("testDataFrameFeatures")
#         print "8888888888888888888888888888888888888888888888800000000000000000000000000000000000008888888889999999999"
#
#         # join = self.sparkSession.sql("select a.index,a.url,a.productId, a.TrainNormFeatures,b.TestNormFeatures from trainDataFrameFeatures a cross Join testDataFrameFeatures b")
#         # print "joinnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn", join.show()
#         # df_temp = self.sparkSession.createDataFrame(join)
#         print "nnnnnnnnnnnnnnnnnnnnnnnnnn"
#         #y = SparkFiles.get("cosine.py")
#         #from y import cosineSimilarity
#         df =  self.trainNormalizeFeatures.withColumn("coSim", udf(cosineSimilarity, FloatType())( col("TrainNormFeatures"),array([lit(v) for v in testDataFrameFeatures['testDataImgFeatures']])))
#         # df = self.sparkSession.createDataFrame([[self.random_dense_vector()] for x in xrange(10)], ["myCol"])
#         # static_vector = self.random_dense_vector()
#         print df.show()
#         # print static_vector
#         print "784565786565947296-7669-5879435672690791111111111111111111111111111111111111111111111111"
#         # df1 = df.withColumn("coSim", udf(cosineSimilarity, FloatType())(col("myCol"), array([lit(v) for v in static_vector])))
#         print "784565786565947296-7669-587"
#         # print df
#         # print df1.show()
#         # print df1.show()
#         # res = df1.limit(10).toPandas()
#         # print res
#         print "QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ"
#         resultss = df.sort(df.coSim.desc())
#         res = resultss.createOrReplaceTempView("test")
#         sqlDF_train = self.sqlContext.sql("select index,url,productId,cosim from test order by cosim desc limit 10")         #.toDF("index","url", "productId", "cosim").coalesce(1).write.mode("overwrite").format("json").save("op.json")
#         self.top_match = sqlDF_train.toPandas().to_dict('records')
#         print self.top_match
#         # return top_match
#         # print result
#
#
#     def getVisualSearch(self,imgUrl):
#         print("inside visual search")
#         logger.info("inside visualsearch")
#         dataframe_features = self.getTestData(imgUrl)
#
#         a=[json.dumps(dataframe_features)]
#         jsonRDD = self.sc.parallelize(a)
#         df = self.sparkSession.read.json(jsonRDD)
#         #df.show()
#         #df.printSchema()
#         to_vector = udf(lambda a: DenseVector(a), VectorUDT())
#         data = df.select("imgUrl", to_vector("testDataImgFeatures").alias("features"))
#         #data.printSchema()
#         #data.show()
#         testNormalizeFeatures = self.getNormalizer(data,"TestNormFeatures")
#         # testNormalizeFeatures.show()
#         self.getCosineSimilarity(dataframe_features)
#
#
#     def getTestData(self,imgUrl):
#         testDataFeature = requests.post('http://192.168.1.150:8011/visualsearch/extractproductfeature/',
#                                       data=json.dumps({'imgUrl': imgUrl})).content
#
#         testDataFeature = json.loads(testDataFeature)
#         testData = {}
#         testData['imgUrl'] = imgUrl
#         testData['testDataImgFeatures'] = (testDataFeature['imgFeatures'])
#         return testData

# if __name__ == '__main__':
#
#     trainFeaturePath = "/tmp/train_no.csv"
#     #print "================================================================data=============================="
#     vs = VISUALSEARCH(trainFeaturePath)
#
#     testImgUrl = "https://s3-ap-southeast-1.amazonaws.com/styfi-image/cache/catalog/products/12519_1493298986865_0-600x750.jpg"
#
#     x = vs.getVisualSearch(testImgUrl)
#
#     print x


@csrf_exempt
def searchByImage(request):
    if request.method == 'POST':
        image_da = json.loads(request.body)
        if image_da:
            try:
                imgUrl = image_da.get('imgUrl', None)
                global vs
                vs = VISUALSEARCH("/tmp/train_no.csv")
            #    vs.sc.addPyFile('reverseimagesearch/spark/cosine.py')
                #Get Image Data
                response = requests.get(imgUrl)
                img = response.content
                imgType = response.headers['Content-Type'].split(';')[0].lower()
                print imgType
                #Download Image from a give URL
                # imgPath = utils.downloadImage(imgUrl)
                # print "imgPath : ", imgPath
                if imgType in ('image/jpg','image/jpeg','application/x-www-form-urlencoded','image/png'):
                        responseDataArray = []
                        print "inside image type"
                        results = vs.getVisualSearch(imgUrl)
                        vs.sc.stop()
                        return HttpResponse(json.dumps({'context_dict': vs.top_match}),
                                content_type='application/json; charset=utf8')
                else:
                    return JsonResponse({'Error' : 'Invalid file type :{} '.format(imgType) })
            except Exception as e :
                print e
                return JsonResponse({'Error': 'unable to processed the image'})
