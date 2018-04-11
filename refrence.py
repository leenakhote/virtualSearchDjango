
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
import pandas as pd
import shlex, subprocess
import pyspark.sql.functions as f
import pyspark.sql.types as t

#from pyspark.sql.functions import pandas_udf

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def dummy_function(data_str):
    print "lala"
    cleaned_str = 'dummyData'
    return cleaned_str

def squared(s):
    print("SQUARE")
    return s * s


class HELLO:

    def cosineSimilarity(a,b):
        print ("hiiiiiieeeeeeeeeeeeeeeeeee")
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

#sample_udf = udf(float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))))

class VISUALSEARCH:
    """A visual Search APPlication
    """

    def __init__(self, feature_dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        import os
        #   from functions import cosineSimilarity
        #from module import function
        #os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.11:0.4.1 pyspark-shell'
        logger.info("Starting up the Recommendation Engine: ")
        self.sc = SparkContext.getOrCreate()
        self.sparkSession = SparkSession(self.sc)
        self.sqlContext = SQLContext(self.sc)

        #s3 = sh.bash.bake("aws s3")
        #s3.put("file","s3n://bucket/file")
        #data = s3  s3://darwin-vs/train_no.csv -
    #    print ("55555555555555555555555",data)
        # command = 'aws s3 cp s3://darwin-vs/train_no.csv -'
        # s3_folder_data  = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
        #print (s3_folder_data)
        # self.sc.hadoopConfiguration.set("fs.s3n.access.key", "AKIAJ6IIHKANYPNE5X6A")
        # self.sc.hadoopConfiguration.set("fs.s3n.secret.key", "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")
        AWS_ACCESS_KEY = 'AKIAJ6IIHKANYPNE5X6A'
        AWS_SECRET_KEY = '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'
        # hadoopConf = self.sc.hadoopConfiguration;
        # hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        # hadoopConf.set("fs.s3.awsAccessKeyId", 'AKIAJ6IIHKANYPNE5X6A')
        # hadoopConf.set("fs.s3.awsSecretAccessKey", '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' )

    #    file = self.sc.addFile("https://s3.ap-south-1.amazonaws.com/darwin-vs/train_no.csv")
        #https://s3.ap-south-1.amazonaws.com/darwin-vs/train_no.csv
        #Data_XMLFile = self.sqlContext.read.format("xml").options(rowTag="anytaghere").load(pyspark.SparkFiles.get("*_public.xml")).coalesce(10).cache()

        logger.info("Loading Features data...")
        #print("77777774444447",feature_dataset_path)
        #featuresRawRDD = self.sc.textFile(s3_folder_data)
        #featuresRawRDD = self.sc.textFile("/home/leena/Downloads/train_no.csv")

        #featuresRawRDD = self.sc.textFile("s3n://"+AWS_ACCESS_KEY+":" + AWS_SECRET_KEY + "/darwin-vs/train_no.csv")
        #featuresRawRDD = self.sc.textFile("s3://darwin-vs/train_no.csv")

        # featuresRawRDD = self.sqlContext.read.load("s3://darwin-vs/train_no.csv",
        #      format='com.databricks.spark.csv',
        #      header='true',
        #      delimiter='\t',
        #      inferSchema='true')
        #
        # featuresRawRDD = sc.textFile("s3://darwin-vs/train_no.csv")

    #    print("88888888888888888888888888",featuresRawRDD.count)

        #print ("888888888888",s3_folder_data)
        # y = self.sc.parallelize(s3_folder_data).collect()
        # print(y)
        # logger.info("Loading Features RDD data...")

        #Map Read CSV file and Map to RDD
        #featuresRdd = featuresRawRDD.mapPartitions(lambda x: csv.reader(x))
        #print("yyyyyyyyyyyy 8888888888888", featuresRdd)
        self.trainNormalizeFeatures = self.formatFeaturesDF(feature_dataset_path,"TrainNormFeatures")

        
    def formatFeaturesDF(self,featuresRawRDD, outputColName):
        #Convert RDD Data into DataFrame
        #print ("inside format", featuresRawRDD)

        # dataFrameFeatures = type(featuresRawRDD)
        # print (dataFrameFeatures)
        x = featuresRawRDD.strip();
        dataFrameFeatures = self.sc.parallelize([x])
        #y = self.sc.parallelize(featuresRawRDD).featuresRawRDD.map(lambda line : line.split("\n")).flatMap(lambda words : (word.split(",") for word in words))
        # print y.collect()
        # print (type(y))
        dataframe_features1 = dataFrameFeatures.map(lambda line : line.split("\r\n"))\
        .flatMap(lambda words : (word.split(",") for word in words)).map(lambda x : [elem.strip('"') for elem in x])\
        .map(lambda x: (x[0], x[1], x[2],DenseVector(x[3:]))).toDF(["index", "url", "productId", "features"])

        #print ("******",dataframe_features1.show())


        # x = RDD[featuresRawRDD]
        # # res1: List[String] = List(featuresRawRDD)
        # print (type(x))
        # list = List(featuresRawRDD)
        # print (type(list))
        #
        # x = list.map(lambda x : x);
        # print( x.collect())
        # # rdd = self.sc.makeRDD(featuresRawRDD)
        # print (type(rdd))
        #print featuresRawRDD.collect()
        # x = featuresRawRDD.map(lambda x : x);
        # print( x.collect())
        # dataframe_features = featuresRawRDD.map(lambda line : line.split("\n")).flatMap(lambda words : (word.split(",") for word in words))\
        # .map(lambda x: (x[0], x[1], x[2],DenseVector(x[3]))).toDF(["index", "url", "productId", "features"])

        #dataframe_features = featuresRawRDD.map(lambda x: (x[0], x[1], x[2],DenseVector((x[3]).split(',')))).toDF(["index", "url", "productId", "features"])
        #dataframe_features = featuresRawRDD.flatMap(lambda words : (word.split(",") for word in words)).map(lambda x: (x[0], x[1], x[2],x[3])).take(5)#.toDF(["index", "url", "productId", "features"])
        #dataframe_features = featuresRawRDD.map(lambda x: (x[0]))
        #print ("******",dataframe_features)
        trainNormalizeFeatures = self.getNormalizer(dataframe_features1,outputColName)
        return trainNormalizeFeatures


    def getNormalizer(self,dataFrameFeatures,outputColName):
        print ("inside normalizer")
        #print (dataFrameFeatures)
        #define Normalizer to get normailize freatures
        normalized = Normalizer(inputCol="features", outputCol=outputColName, p=2.0)
        #Get Normalize feature
        normData = normalized.transform(dataFrameFeatures);
        return normData

    def cosineSimilarity(a,b):
        print("inside cosine")
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


    #@udf('float')
    # def cdf(a,b):
    #     print ("inside")
    #     y = float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
    #     print y
    #     return self.sc.broadcast(y)


    # def multiAdd : udf[Double,Row](r => {
    #     n = 0.0
    #     r.toSeq.foreach(n1 => n = n + (n1 match {
    #         case l: Long => l.toDouble
    #         case i: Int => i.toDouble
    #         case d: Double => d
    #         case f: Float => f.toDouble
    #     }))
    #   n
    # })

    def squared(s):
        print("SQUARE")
        return s * s

    def increment(x):
        print("increment")
        return x + 1

    def dummy_function(data_str):
        print "lala"
        cleaned_str = 'dummyData'
        return cleaned_str


    def getCosineSimilarity(self,testDataFrameFeatures):
        #Create Table for temporary View for Train DAta
        trainDataFrameFeaturesNorm = self.trainNormalizeFeatures.createOrReplaceTempView("trainDataFrameFeatures")
        #Create Table for temporary View for TEst DAta
        testDataFrameFeaturesNorm = testDataFrameFeatures.createOrReplaceTempView("testDataFrameFeatures")
        join = self.sparkSession.sql("select a.index,a.url,a.productId, a.TrainNormFeatures,b.TestNormFeatures from trainDataFrameFeatures a cross Join testDataFrameFeatures b")
        print join.show()

        y = HELLO.cosineSimilarity(16, 77.66)
        print y

        #join.cache()
        # self.sqlContext.udf.register("squaredWithPython", squared)
        # squared_udf = udf(squared, LongType())
        # df = (join.select("index", squared_udf("index").alias("id_squared")))
        # print df.show()
        #dummy_function_udf = self.dummy_function('yy')
        # df = join.withColumn("dummy", self.dummy_function(join.TestNormFeatures))
        # print df.show()
        # training_df = sqlContext.sql("select 'foo' as tweet, 'bar' as classification")
        # print training_df.show()

        # df = Seq((1.0,2),(3.0,4)).toDF("c1","c2")
        # df.withColumn("add", multiAdd(struct($"c1", $"c2"))).show
        # data = float(np.dot(join.TrainNormFeatures, join.TestNormFeatures) / (np.linalg.norm(join.TrainNormFeatures) * np.linalg.norm(join.TestNormFeatures)))
        # print data
        #df = join.withColumn("coSim", data)
        #df = join.withColumn("coSim", "hii")
        # data = self.cosineSimilarity( FloatType())(join.TrainNormFeatures,join.TestNormFeatures)
        # print data
        #cosineSim = self.sc.broadcast(cosineSimilarity)
        print("jjjjjjjj")
        #df = join.withColumn('coSim', self.cdf(FloatType())(join.TrainNormFeatures,join.TestNormFeatures))

        # float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
        #myUDF = udf(self.cosineSimilarity)

        #newDF = df.withColumn("newCol", myUDF(df("oldCol")))
        # data = 123
        # df = join.withColumn("coSim", lit(data))
        #y = float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
        #df.withColumn("sample",sample_udf(col("text")))
        #df = join.withColumn("coSim", (sample_udf(join.TrainNormFeatures,join.TestNormFeatures)))
        # increment_udf = f.udf(self.increment)
        # df = join.withColumn('coSim', increment_udf (join.TrainNormFeatures))

        #data_udf(a,b) = float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

        # addOneByRegister = self.sqlContext.udf.register("squaredWithPython", self.squared)
        # #addOneByRegister = self.sqlContext.udf.register("coSim", self.cosineSimilarity(join.TrainNormFeatures,join.TestNormFeatures))
        # print addOneByRegister.show()
        # # increment_udf = f.udf(self.cosineSimilarity, t.FloatType())
        #
        # df = join.withColumn("coSim", increment_udf(join.TrainNormFeatures,join.TestNormFeatures))
        print("hi")
        #print df.show()
        # pairs = distinct_users_projected.map(lambda x: (x.user_id, pt.broadcast_products_lookup_map.value[x.prod_id]))
        # bcast = pt.broadcast_products_lookup_map pairs = distinct_users_projected.map(lambda x: (x.user_id, bcast.value[x.prod_id]))
        # resultss = df.sort(df.coSim.desc())
        # res = resultss.createOrReplaceTempView("test")
        # sqlDF_train = self.sqlContext.sql("select index,url,productId,cosim from test order by cosim desc limit 10")                #.toDF("index","url", "productId", "cosim").coalesce(1).write.mode("overwrite").format("json").save("op.json")
        # print sqlDF_train.show()
        # self.top_match = sqlDF_train.toPandas().to_dict('records')
        self.top_match = "hi"

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
        testDataFeature = requests.post('http://ares.styfi.in:81/visualsearch/extractproductfeature/',
                                      data=json.dumps({'imgUrl': imgUrl})).content

        testDataFeature = json.loads(testDataFeature)
        testData = {}
        testData['imgUrl'] = imgUrl
        testData['testDataImgFeatures'] = (testDataFeature['imgFeatures'])
        #print ("*******************************", testData)
        return testData
#
# if __name__ == '__main__':
#
#     trainFeaturePath = "/home/leena/dev/visualsearch/train_no.csv"
#
#     vs = VISUALSEARCH(trainFeaturePath)
#
#     testImgUrl = "https://s3-ap-southeast-1.amazonaws.com/styfi-image/cache/catalog/products/12519_1493298986865_0-600x750.jpg"
#
#     vs.getVisualSearch(testImgUrl)

    #print vs.result.show()
