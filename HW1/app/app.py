import preprocess as pre
import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

def get_basic_statistics(column_name):
    max = df.agg({column_name: "max"}).collect()[0][f"max({column_name})"]
    min = df.agg({column_name: "min"}).collect()[0][f"min({column_name})"]
    return max,min

conf = SparkConf().setAppName('hello')
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.getOrCreate()

df = spark.read.csv("preprocessed/pre_household_power_consumption.csv",header=True,sep=",")
count = df.count()

global_active_power_stat = get_basic_statistics("Global_active_power")
global_reactive_power_stat = get_basic_statistics("Global_reactive_power")
voltage_stat = get_basic_statistics("Voltage")
global_intensity_stat= get_basic_statistics("Global_intensity")



