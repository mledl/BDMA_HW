import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import regexp_replace, split, lower, col
from pyspark.sql.types import *
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql import Window

#######################################################################################################################

run_spark_in_cluster = False      # SET THIS VARIABLE FOR TESTING VS PRODUCTION

k = 2  # parameter for number of k-shingles

link_to_cluster_storage = "hdfs://namenode:9000"
link_to_local_storage = "../data/"
if(run_spark_in_cluster):
    path_to_write = ""
else:
    path_to_write = link_to_local_storage

#######################################################################################################################



#######################################################################################################################

if(run_spark_in_cluster):
    spark = SparkSession.builder.appName('hw4').master('spark://spark-master:7077')
else:
    spark = SparkSession.builder.appName('hw4').master('local')

spark = spark.getOrCreate()
sqlContext = SQLContext(spark)



### DATA CLEANING

