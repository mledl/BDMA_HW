import numpy as np		# handle number
import pandas as pd		# handle dataset
import os

from sklearn.preprocessing import LabelEncoder, OneHotEncoder 	# handle data encoding
from sklearn.impute import SimpleImputer 						# handle missing data
from sklearn.preprocessing import StandardScaler 				# feature scaling
from sklearn.model_selection import train_test_split 			# splitting training and testing data

from zipfile import ZipFile	#extract from zip

def cleanup():
	#delete tmp directory and file(s)
	tmpFileList = [ f for f in os.listdir(tmpDir) if f.endswith(".txt") ]
	for f in tmpFileList:
		os.remove(os.path.join(tmpDir, f))
	
	os.rmdir(tmpDir)

# preprocessing attributes
zipFileName = 'household_power_consumption.zip';
zipFilePath = '../datasets/' + zipFileName;
tmpDir = '../datasets/tmp/';
targetFileName = 'pre_household_power_consumption.csv';

print('extracting file: ' + zipFilePath)

# extract dataset zip file into tmp directory
with ZipFile(zipFilePath, 'r') as zipObj:
	fileNames = zipObj.namelist()
	print('files in archive: ' + str(fileNames))
	for fileName in fileNames:
		if fileName.endswith('.txt'):
			zipObj.extract(fileName, '../datasets/tmp')
			print('successfully extracted file: ' + fileName)
			
# read extracted dataset, missing values are represented as ?
try:
	raw_data = pd.read_csv(tmpDir + 'household_power_consumption.txt', sep=';', na_values=['?']);
except:
	print('failed to load file: ' + tmpDir + 'household_power_consumption.txt')
	cleanup();

# dimension reduction, needed attributes: 
data = raw_data.drop(['Date', 'Time', 'Sub_metering_1', 'Sub_metering_2', 'Sub_metering_3'], 1)

# check amount of zero/missing values
print('number of null values per column: ')
print(data.isnull().sum())

# replace missing values
data['Global_active_power'].fillna(data['Global_active_power'].median(), inplace=True)
data['Global_reactive_power'].fillna(data['Global_reactive_power'].median(), inplace=True)
data['Voltage'].fillna(data['Voltage'].median(), inplace=True)
data['Global_intensity'].fillna(data['Global_intensity'].median(), inplace=True)

# print info of dataset
print ('dataset info:')
data.info()
print('\n')

data.to_csv('../preprocessed/' + targetFileName)

# cleanup temporarily generated data
cleanup()