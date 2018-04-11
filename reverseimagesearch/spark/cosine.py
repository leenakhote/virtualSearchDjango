

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

def cosineSimilarity(a,b):
    print a ,b
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
