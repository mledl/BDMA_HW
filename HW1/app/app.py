import preprocess as pre
import findspark
findspark.init()

from pyspark import SparkConf, SparkContext

pre.preprocess_data('household_power_consumption')

conf = SparkConf().setAppName('hello').setMaster('spark://172.21.0.2:7077').setSparkHome('/spark/')
sc = SparkContext(conf=conf)

nums= sc.parallelize([1,2,3,4])
num = nums.take(2)
print(num)