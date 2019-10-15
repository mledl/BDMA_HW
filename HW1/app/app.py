import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler


def get_basic_statistics(data, column_name):
    max = data.agg({column_name: "max"}).collect()[0][0]
    min = data.agg({column_name: "min"}).collect()[0][0]
    return max,min

# Spark setup
conf = SparkConf().setAppName('app').setMaster('spark://172.18.0.3:7077').setSparkHome('/spark/')
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.getOrCreate()

# Reading csv
df = spark.read.csv("hdfs://namenode:8020/pre_household_power_consumption.csv",header=True,sep=",", inferSchema=True)

# Calculating statistics
count = df.count()

global_active_power_stat = get_basic_statistics(df, "Global_active_power")
global_reactive_power_stat = get_basic_statistics(df, "Global_reactive_power")
voltage_stat = get_basic_statistics(df, "Voltage")
global_intensity_stat= get_basic_statistics(df, "Global_intensity")

# Calculating Min-max normalization
assembler = VectorAssembler(inputCols=df.columns[1:], outputCol="features")
df_2 = assembler.transform(df)

scaler = MinMaxScaler(min=0, max=1, inputCol='features', outputCol='features_minmax')
scaler_model = scaler.fit(df_2)
df_3 = scaler_model.transform(df_2)

# Transforming Dense vector to dataframe    `
min_max_df = df_3.rdd.map(lambda x:[float(y) for y in x['features_minmax']]).toDF(df.columns[1:])

print("global_active_power_stat", global_active_power_stat)




