# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.shortcuts import render
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse,HttpResponse
import json
import requests
import boto3
import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.session import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark import sql

from udf import *

# Create your views here.
# sc = SparkContext.getOrCreate()
# sparkSession = SparkSession(sc)
# sqlContext = SQLContext(sc)

# vs = VS()


# trainData = loadTrainData(vs.sc, vs.sparkSession ,vs.sqlContext)

#

# print y

@csrf_exempt
def searchByImage(request):
    if request.method == 'POST':
        image_da = json.loads(request.body)
        print image_da
        if image_da:
            try:
                print("data post")
                imgUrl = image_da.get('imgUrl', None)
                response = requests.get(imgUrl)
                img = response.content
                imgType = response.headers['Content-Type'].split(';')[0].lower()
                #print imgType
                if imgType in ('image/jpg','image/jpeg','application/x-www-form-urlencoded','image/png'):
                    #print "inside image type"
                    # results = getVisualSearch(imgUrl, trainData)
                    sc = SparkContext.getOrCreate()
                    sparkSession = SparkSession(sc)
                    sqlContext = SQLContext(sc)
                    results = trainData(imgUrl,sc,sparkSession,sqlContext)
                    print results
                    # #vs.sc.stop()
                    return HttpResponse(json.dumps({'context_dict': results}),
                                content_type='application/json; charset=utf8')
                else:
                    return JsonResponse({'Error' : 'Invalid file type :{} '.format(imgType) })
            except Exception as e :
                print (e)
                return JsonResponse({'Error': 'unable to processed the image'})
