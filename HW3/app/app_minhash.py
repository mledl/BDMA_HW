import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import random
import numpy as np
from pyspark.accumulators import AccumulatorParam

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

df = spark.read.csv(path_to_write + "results/task1/task1.csv", header=True, sep=" ", inferSchema=True)

prime = 110641  # bigger than number of rows http://compoasso.free.fr/primelistweb/page/prime/liste_online_en.php
numHashes = 100
maxShingleNumber = df.count()


def pickRandomNumbers(k):
    randList = []

    for i in range(k):
        randIndex = random.randint(0, maxShingleNumber)
        while randIndex in randList:
            randIndex = random.randint(0, maxShingleNumber)
        randList.append(randIndex)

    return randList


# hash functions form of: h(x) = (a*x + b) % c
def calcualte_hash(a, b, shingleID):
    hashCode = (a * shingleID + b) % prime
    return hashCode


coeffA = pickRandomNumbers(numHashes)
coeffB = pickRandomNumbers(numHashes)
print("h(x) = (a*x + b) % c where a=", coeffA, ",b=", coeffB, ",c=", prime)


class MatrixAccumulatorParam(AccumulatorParam):
    def zero(self, matrix):
        return matrix

    def addInPlace(self, matrix, v_param):
        if isinstance(v_param, np.ndarray):
            return v_param
        elif v_param['hash'] < matrix.item((v_param['row'], v_param['col'])):
            matrix.itemset((v_param['row'], v_param['col']), v_param['hash'])
        return matrix


# signatures = [[prime + 1 for i in range(len(df.columns))] for j in range(numHashes)]
signatures = np.full((numHashes, len(df.columns) - 1), prime + 1)
signaturesAccum = sc.accumulator(signatures, MatrixAccumulatorParam())


def calcualte_signatures(elem):
    (row, index) = elem

    if index % 10000 == 0:
        print(index)

    hash_values = []
    for i in range(numHashes):
        hash_values.append(calcualte_hash(coeffA[i], coeffB[i], index + 1))

    row_dic = row.asDict()
    del row_dic["_1"]

    for col, key in enumerate(row_dic):
        if row_dic[key] == 1:
            for row, hash_value in enumerate(hash_values):
                signaturesAccum.add({'row': row, 'col': col, 'hash': hash_value})


df.rdd.zipWithIndex() \
    .foreach(calcualte_signatures)

print(signaturesAccum.value)
np.savetxt(path_to_write + "results/signatures.txt", signaturesAccum.value)
