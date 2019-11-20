import findspark
findspark.init()
from collections import defaultdict
from typing import Tuple, List

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import regexp_replace, split, lower, col
from pyspark.sql.types import *
import pyspark.sql.functions as f

#######################################################################################################################

run_spark_in_cluster = False      # SET THIS VARIABLE FOR TESTING VS PRODUCTION

k = 5  # parameter for number of k-shingles

link_to_cluster_storage = "hdfs://namenode:9000"
link_to_local_storage = "C:/Users/Tobias/Docker/BDMA_HW"
if(run_spark_in_cluster):
    path_to_write = ""
else:
    path_to_write = link_to_local_storage

#######################################################################################################################

def shingling(element, k):
    elem = np.array(element)
    for i in range(elem[0].size - k + 1):
        shingle = ""
        for j in range(i, i+k):
            if j < (i+k):
                shingle = shingle + str(elem[0][j]) + " "
            else:
                shingle = shingle + str(elem[0][j])
        yield shingle    # yield can return a sequence of objects/results


#######################################################################################################################

if(run_spark_in_cluster):
    spark = SparkSession.builder.appName('hw2').master('spark://spark-master:7077').getOrCreate()
    sqlContext = SQLContext(spark)
    # infer Schema important in order to realize that numbers a not a string
    #df = spark.read.csv(link_to_cluster_storage + "/household_power_consumption.csv", sep=",", inferSchema=True, header=True)
else:
    spark = SparkSession.builder \
        .appName('hw3') \
        .master('local') \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.7.0") \
        .config("spark.driver.maxResultSize", "20g") \
        .getOrCreate()
    sqlContext = SQLContext(spark)

df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rootTag", "<!DOCTYPE") \
    .option("rowTag", "BODY") \
    .load(path_to_write + "/HW3/data/*")

### DATA CLEANING
df = df.withColumn('content', regexp_replace('_corrupt_record', '\n', ' ')) # replace newline by one whitespace
df = df.select('content').withColumn('content', lower(col('content'))) # text to lowercase
df = df.withColumn('content', regexp_replace('content', '<body>', ''))  # replace <BODY>
df = df.withColumn('content', regexp_replace('content', '[\\?,\\.,\\$]', ' ')) # replace ? , . and $ by whitespace
df = df.withColumn('content', regexp_replace('content', ' +', ' ')) # replace multiple whitespaces by one
df = df.withColumn('content', regexp_replace('content', ' reuter &#3;</body>', '')) # replace BODY tag at end of document
df_split = df.withColumn('content', split(col('content'), " ").alias('content'))


### CREATE SHINGLES
result_shingle = df_split.rdd.flatMap(lambda x: shingling(x, k)).distinct()
document_list = df.collect()

df_matrix = result_shingle.map(lambda x: ["1" if str(x) in str(document) else "0" for document in document_list]).toDF()
df_matrix.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save(path_to_write + "/HW3/data/results/task1.csv", header='true', sep=" ")
