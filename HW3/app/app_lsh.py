import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import random
import numpy as np
from pyspark.accumulators import AccumulatorParam
import itertools
import psutil

#######################################################################################################################

run_spark_in_cluster = False  # SET THIS VARIABLE FOR TESTING VS PRODUCTION

link_to_cluster_storage = "hdfs://namenode:9000"
link_to_local_storage = "../data/"
if (run_spark_in_cluster):
    path_to_write = ""
else:
    path_to_write = link_to_local_storage

if (run_spark_in_cluster):
    conf = SparkConf().setAppName('hw3').setMaster('spark://spark-master:7077')
else:
    conf = SparkConf().setAppName('hw3').setMaster('local')

sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.getOrCreate()

df = spark.read.csv(path_to_write + "results/signatures.txt", header=False, sep=" ", inferSchema=True)
df.show()

BAND_SIZE = 1

partitions = round(df.count() / BAND_SIZE)
threshold = 0.1
rdd = df.repartition(partitions).rdd

print("Number of partitions:", partitions)
print("Threshold:", threshold)


def get_buckets_per_doc(band):
    signatures_bands = {}

    for row in band:
        for index_col, value_col in enumerate(list(row)):
            key = 'doc_' + str(index_col)
            if key in signatures_bands:
                signatures_bands[key] = signatures_bands[key] + str(value_col)
            else:
                signatures_bands[key] = str(value_col)

    return signatures_bands.items()


doc_buckets = rdd.mapPartitions(get_buckets_per_doc) \
    .groupByKey() \
    .mapValues(list) \
    .collect()


def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3


threshold = threshold * len(df.columns)
print("Pairs:")
for a, b in itertools.combinations(doc_buckets, 2):
    (db_i_doc, db_i_buckets) = a
    (db_j_doc, db_j_buckets) = b

    intersection_docs = intersection(db_i_buckets, db_j_buckets)

    if len(intersection_docs) >= threshold and db_j_doc != db_i_doc:
        print("Pair", db_i_doc, db_j_doc, len(intersection_docs))
