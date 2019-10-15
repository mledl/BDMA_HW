import glob, os
import numpy as np
import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as fun
from pyspark.ml.feature import MinMaxScaler, VectorAssembler, Imputer

from zipfile import ZipFile

def extract_dataset(zipFilePath, tmpDir):
	# extract dataset zip file into tmp directory
	with ZipFile(zipFilePath, 'r') as zipObj:
		fileNames = zipObj.namelist()
		print('files in archive: ' + str(fileNames))
		for fileName in fileNames:
			if fileName.endswith('.txt'):
				zipObj.extract(fileName, tmpDir)
				pre, ext = os.path.splitext(fileName)
				os.rename(tmpDir + fileName, tmpDir + pre + '.csv')
				print('successfully extracted file: ' + fileName)

def prepocess_data(df):
	# Preprocessing the data
	# Dimension reduction
	cols_reduce = ['Date', 'Time', 'Sub_metering_1', 'Sub_metering_2', 'Sub_metering_3'];
	df = df.drop(*cols_reduce)
	
	# Fixing missing values (dataset uses ? as NaN for missing values)
	imputer = Imputer(inputCols = df.columns, outputCols = df.columns)
	imputer.setStrategy("mean")
	df = imputer.fit(df).transform(df)
	
	# Print the column name and datatype
	print(df.dtypes)
	return df

def cleanup(dir, del_dir):
	#delete tmp directory and file(s)
	tmpFileList = [ f for f in os.listdir(dir)]
	for f in tmpFileList:
		os.remove(os.path.join(dir, f))
	
	if(del_dir == 1):
		os.rmdir(dir)
		
	print('successfully deleted directory: ' + dir)


def get_basic_statistics(df, column_name):
	max = df.agg({column_name: "max"}).collect()[0][f"max({column_name})"]
	min = df.agg({column_name: "min"}).collect()[0][f"min({column_name})"]
	df_stats = df.select(fun.mean(fun.col(column_name)).alias('mean'), fun.stddev(fun.col(column_name)).alias('std')).collect()
	mean = df_stats[0]['mean']
	std = df_stats[0]['std']
	
	return column_name, df.count(), min, max, mean, std
	
def print_statistics(stats, norm_data):
	df_print = spark.createDataFrame(stats, ['column_name', 'count', 'min', 'max', 'mean', 'std'])
	df_print.show()
	
	if os.path.exists('../results'):
		cleanup('../results', 0)
	
	df_print.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode('overwrite').save("../results/tmp_stats")
	for fileName in glob.glob('../results/tmp_stats/part-0000*.csv'):
		os.rename(fileName, '../results/hw1_stats.csv')
		print('successfully created ../results/hw1_stats.csv')
	
	cleanup("../results/tmp_stats", 1)
	
	new_col_names = ['Norm_global_active_power', 'Norm_global_reactive_power', 'Norm_voltage', 'Norm_global_intensity'];
	norm_data_print = norm_data.toDF(*new_col_names)
	norm_data_print.write.format("com.databricks.spark.csv").option("header", "true").mode('append').save("../results/tmp_norm")
	merge_files('../results/tmp_norm/part-0*.csv')
	cleanup('../results/tmp_norm', 1)
		
def merge_files(files):
	first = 1;
	with open('../results/hw1_min_max_normalization.csv', 'a') as targetFile:
		print(glob.glob(files))
		for fileName in glob.glob(files):
			with open(fileName) as sourceFile:
				m = sourceFile.readlines()
				if first == 1:
					lines = m[0:];
					first = 0;
				else:
					lines = m[1:];
					
				for line in lines:
					targetFile.write(line.rstrip() + '\n')
				
	print('successfully merged data into file: ../results/hw1_min_max_normalization.csv')
		

# Spark setup
conf = SparkConf().setAppName('hello')
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.getOrCreate()

# Extracting datset and reading data
tmpDir = '../data/tmp/';
extract_dataset("../data/household_power_consumption.zip", tmpDir)
df = spark.read.csv(tmpDir + "household_power_consumption.csv", header=True, sep=";", inferSchema=True, nullValue="?")

# Data preprocessing
df = prepocess_data(df)

# Calculating statistics
global_active_power_stat = get_basic_statistics(df, "Global_active_power")
global_reactive_power_stat = get_basic_statistics(df, "Global_reactive_power")
voltage_stat = get_basic_statistics(df, "Voltage")
global_intensity_stat= get_basic_statistics(df, "Global_intensity")

# Calculating Min-max normalization
assembler = VectorAssembler(inputCols=df.columns[0:], outputCol="features")
df_2 = assembler.transform(df)

scaler = MinMaxScaler(min=0, max=1, inputCol='features', outputCol='features_minmax')
scaler_model = scaler.fit(df_2)
df_3 = scaler_model.transform(df_2)

# Transforming Dense vector to dataframe
min_max_df = df_3.rdd.map(lambda x:[float(y) for y in x['features_minmax']]).toDF(df.columns[0:])

# create files and print
print_statistics([global_active_power_stat, global_reactive_power_stat, voltage_stat, global_intensity_stat], min_max_df)

cleanup(tmpDir, 1)


