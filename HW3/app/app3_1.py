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

def shingling(element, k):
    for i in range(len(element[0]) - k + 1):
        shingle = ""
        doc_nr = int(element[1])
        for j in range(i, i+k):
            if j < (i+k-1):
                shingle = shingle + str(element[0][j]) + " "
            else:
                shingle = shingle + str(element[0][j])
        yield shingle, doc_nr   # key, value     # yield can return a sequence of objects/results


def convert_Output(x, doc_count):
    out = []
    out.append(x[0])
    index = 0
    list_len = len(x[1])

    for i in range(doc_count):   # iterating through from 0 to (doc_count -1)
        if index < list_len:
            doc = int(x[1][index])     # document number
            if doc == i:
                out.append(1)
                index = index + 1
            else:
                out.append(0)
        else:
            out.append(0)
    return out


#######################################################################################################################

if(run_spark_in_cluster):
    spark = SparkSession.builder.appName('hw3').master('spark://spark-master:7077')
else:
    spark = SparkSession.builder.appName('hw3').master('local')

spark = spark.getOrCreate()

sqlContext = SQLContext(spark)
df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rootTag", "<!DOCTYPE") \
    .option("rowTag", "BODY") \
    .load(path_to_write + "test/*")


### DATA CLEANING
df = df.withColumn('content', regexp_replace('_corrupt_record', '\n', ' ')) # replace newline by one whitespace
df = df.select('content').withColumn('content', lower(col('content'))) # text to lowercase
df = df.withColumn('content', regexp_replace('content', '<body>', ''))  # replace <BODY>
df = df.withColumn('content', regexp_replace('content', '[\\?,\\.,\\$]', ' ')) # replace ? , . and $ by whitespace
df = df.withColumn('content', regexp_replace('content', '\r', ' '))
df = df.withColumn('content', regexp_replace('content', ' +', ' ')) # replace multiple whitespaces by one
df = df.withColumn('content', regexp_replace('content', ' reuter &#3;</body>', '')) # replace BODY tag at end of document
df_split = df.withColumn('content', split(col('content'), " ").alias('content'))
df_split_id = df_split.withColumn("id", row_number().over(Window.orderBy(monotonically_increasing_id()))-1)


### CREATE SHINGLE SET REPRESENTATION
result_shingle = df_split_id.rdd.flatMap(lambda x: shingling(x, k)).distinct() \
    .groupByKey() \
    .map(lambda x: (x[0], list(x[1])))   # grouping the values together for each unique shingle

doc_count = df.count()
print("Number of documents: " + str(doc_count))

df_matrix = result_shingle.map(lambda x: convert_Output(x, doc_count)).toDF()
df_matrix.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save(path_to_write + "results/task1", header='true', sep=" ")
