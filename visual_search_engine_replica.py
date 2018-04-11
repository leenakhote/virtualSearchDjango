
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


import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def cosineSimilarity(a,b):
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

class VISUALSEARCH:
    """A visual Search APPlication
    """
    def __init__(self, feature_dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        import os
        #os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.11:0.4.1 pyspark-shell'
        logger.info("Starting up the Recommendation Engine: ")
        self.sc = SparkContext.getOrCreate()
        self.sparkSession = SparkSession(self.sc)
        self.sqlContext = SQLContext(self.sc)
        command = 'aws s3 cp s3://darwin-vs/train_no1.csv -'
        s3_folder_data  = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
        AWS_ACCESS_KEY = 'AKIAJ6IIHKANYPNE5X6A'
        AWS_SECRET_KEY = '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'

        logger.info("Loading Features data...")

        self.trainNormalizeFeatures = self.formatFeaturesDF(s3_folder_data,"TrainNormFeatures")


    def formatFeaturesDF(self,featuresRawRDD, outputColName):
        x = featuresRawRDD.strip();
        dataFrameFeatures = self.sc.parallelize([x])

        dataframe_features1 = dataFrameFeatures.map(lambda line : line.split("\r\n"))\
        .flatMap(lambda words : (word.split(",") for word in words)).map(lambda x : [elem.strip('"') for elem in x])\
        .map(lambda x: (x[0], x[1], x[2],DenseVector(x[3:]))).toDF(["index", "url", "productId", "features"])

        trainNormalizeFeatures = self.getNormalizer(dataframe_features1,outputColName)
        return trainNormalizeFeatures


    def getNormalizer(self,dataFrameFeatures,outputColName):
        print ("inside normalizer")
        print (dataFrameFeatures)
        #define Normalizer to get normailize freatures
        normalized = Normalizer(inputCol="features", outputCol=outputColName, p=2.0)
        #Get Normalize feature
        normData = normalized.transform(dataFrameFeatures);
        return normData


    def getCosineSimilarity(self,testDataFrameFeatures):

        #Create Table for temporary View for Train DAta
        trainDataFrameFeaturesNorm = self.trainNormalizeFeatures.createOrReplaceTempView("trainDataFrameFeatures")
        #Create Table for temporary View for TEst DAta
        testDataFrameFeaturesNorm = testDataFrameFeatures.createOrReplaceTempView("testDataFrameFeatures")
        join = self.sparkSession.sql("select a.index,a.url,a.productId, a.TrainNormFeatures,b.TestNormFeatures from trainDataFrameFeatures a cross Join testDataFrameFeatures b")
        # join.show()
        sum_cols = udf(lambda x: x[0]+x[1], IntegerType())
        df = join.withColumn("coSim", udf(cosineSimilarity, FloatType())(join.TrainNormFeatures,join.TestNormFeatures))
        #print df.show()
        resultss = df.sort(df.coSim.desc())
        res = resultss.createOrReplaceTempView("test")
        sqlDF_train = self.sqlContext.sql("select index,url,productId,cosim from test order by cosim desc limit 10")         #.toDF("index","url", "productId", "cosim").coalesce(1).write.mode("overwrite").format("json").save("op.json")
        self.top_match = sqlDF_train.toPandas().to_dict('records')
        #print self.top_match

    def getVisualSearch(self,imgUrl):
        print("inside visual search")
        logger.info("inside visualsearch")
        dataframe_features = self.getTestData(imgUrl)

        a=[json.dumps(dataframe_features)]
        jsonRDD = self.sc.parallelize(a)
        df = self.sparkSession.read.json(jsonRDD)

        to_vector = udf(lambda a: DenseVector(a), VectorUDT())
        data = df.select("imgUrl", to_vector("testDataImgFeatures").alias("features"))

        testNormalizeFeatures = self.getNormalizer(data,"TestNormFeatures")

        self.getCosineSimilarity(testNormalizeFeatures)


    def getTestData(self,imgUrl):
        testDataFeature = requests.post('http://192.168.1.200:8011/visualsearch/extractproductfeature/',
                                      data=json.dumps({'imgUrl': imgUrl})).content

        testDataFeature = json.loads(testDataFeature)
        testData = {}
        testData['imgUrl'] = imgUrl
        testData['testDataImgFeatures'] = (testDataFeature['imgFeatures'])
        return testData
#
if __name__ == '__main__':

    trainFeaturePath = "/home/leena/dev/visualsearch/train_no.csv"

    vs = VISUALSEARCH(trainFeaturePath)

    testImgUrl = "https://s3-ap-southeast-1.amazonaws.com/styfi-image/cache/catalog/products/12519_1493298986865_0-600x750.jpg"

    vs.getVisualSearch(testImgUrl)

    print vs.result.show()
