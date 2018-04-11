import requests
import json
from pyspark.ml.linalg import DenseVector
from pyspark.ml.feature import Normalizer
from pyspark import SparkConf, SparkContext  #from pacakge import classes/function
from pyspark.sql import SQLContext, Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, FloatType, DoubleType
import pickle
import csv
import numpy as np
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors, VectorUDT

# sc = spark.sparkContext
spark = SparkContext.getOrCreate()
sparkSess = SparkSession(spark)
sqlContext = SQLContext(spark)


def writeToCsv1(filePath,csvData):
    with open(filePath+".csv", 'w') as csvfile:
        fieldnames = ['url', 'features']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        #writer.writeheader()
        writer.writerows(csvData)
        print("writing complete")

def writeToCsv(filePath,csvData):
    '''
    csvData takes a list and filepath of the csv as input
    note : pass the filename only dont pass the filename with extension as extension is appended inside the function
    '''
    with open(filePath+".csv", 'wb') as csvfile:
       writer = csv.writer(csvfile,
                                quoting=csv.QUOTE_MINIMAL)
       print csvData
       for row in csvData:
           writer.writerow([row])

def getTestData(imgUrl):
    testDataFeature = requests.post('http://ares.styfi.in:81/vs/getproductfeatures/',
                                  data=json.dumps({'imgUrl': imgUrl})).content

    testDataFeature = json.loads(testDataFeature)
    testData = {}
    testData['testDataImgFeatures'] = (testDataFeature['imgFeatures'])
    testData['imgUrl'] = imgUrl
    a=[json.dumps(testData)]
    jsonRDD = spark.parallelize(a)
    df = sparkSess.read.json(jsonRDD)
    df.show()
    df.printSchema()
    to_vector = udf(lambda a: DenseVector(a), VectorUDT())
    data = df.select("imgUrl", to_vector("testDataImgFeatures").alias("features"))
    data.printSchema()
    data.show()
    #stestDataFeature = testDataFeature.items()

    normalizer = Normalizer(inputCol="features", outputCol="normFeatures_test", p=2.0)
    l1NormData1 = normalizer.transform(data);
    l1NormData1.show()

    # rddTrain = spark.textFile("/home/leena/dev/visualsearch/testFinal.csv")
    # #rddTrain = rddTrain.mapPartitions(lambda x: csv.reader(x))
    # #print rddTrain.collect()
    # Result_train = rddTrain.map(lambda x: x[0])

    #Result_test = rddTrain.map(lambda line : line.split("\n")).flatMap(lambda words : [word.split(",") for word in words]).zipWithIndex();
    #print Result_test.show()
    #print Result_train.collect()

    # df = sqlContext.createDataFrame([json.loads(y) for line in y.iter_lines()])
    # df.show()
    j = {'abc':1, 'def':2, 'ghi':3}
    a=[json.dumps(j)]
    jsonRDD = spark.parallelize(a)
    df = sparkSess.read.json(jsonRDD)
    df.show()

    #writeToCsv("/home/leena/dev/visualsearch/testFinal",testDataFeature)

#
# schema =[
#         StructField("URL", StringType(), True),
#         StructField("Features", DoubleType(), True)]
#
#     df = sparkSess.createDataFrame(jsonRDD, schema)
#
#     df.show()

    # #print y
    # df = spark.parallelize(y).toDF()
    # df.show()
    # otherPeople = sparkSess.read.json(y)
    # otherPeople.show()

    return ""


if __name__ == '__main__':
    x = getTestData("http://www.dadacart.com/wp-content/uploads/2017/05/71A2pxQc3XL._UL1500_.jpg")
    #print x
