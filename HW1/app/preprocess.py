import numpy as np		# handle number
import pandas as pd		# handle dataset
import os

from zipfile import ZipFile	#extract from zip

def cleanup(dir):
	#delete tmp directory and file(s)
	tmpFileList = [ f for f in os.listdir(dir) if f.endswith(".txt") ]
	for f in tmpFileList:
		os.remove(os.path.join(dir, f))
	
	os.rmdir(dir)
	print('successfully deleted directory: ' + dir)

def preprocess_data(dataFileName):
	# preprocessing attributes
	zipFileName = dataFileName + '.zip';
	txtFileName = dataFileName + '.txt';
	zipFilePath = '../data/' + zipFileName;
	tmpDir = '../data/tmp/';
	targetFileName = 'pre_' + dataFileName + '.csv';
	
	print('extracting file: ' + zipFilePath)
	
	# extract dataset zip file into tmp directory
	with ZipFile(zipFilePath, 'r') as zipObj:
		fileNames = zipObj.namelist()
		print('files in archive: ' + str(fileNames))
		for fileName in fileNames:
			if fileName.endswith('.txt'):
				zipObj.extract(fileName, tmpDir)
				print('successfully extracted file: ' + fileName)
				
	# read extracted dataset, missing values are represented as ?
	try:
		raw_data = pd.read_csv(tmpDir + txtFileName, sep=';', na_values=['?']);
	except:
		print('failed to load file: ' + tmpDir + txtFileName)
		cleanup(tmpDir);
	
	# dimension reduction, needed attributes: 
	data = raw_data.drop(['Date', 'Time', 'Sub_metering_1', 'Sub_metering_2', 'Sub_metering_3'], 1)
	
	# check amount of zero/missing values
	print('number of null values per column: ')
	print(data.isnull().sum())
	
	# replace missing values
	data['Global_active_power'].fillna(data['Global_active_power'].mean(), inplace=True)
	data['Global_reactive_power'].fillna(data['Global_reactive_power'].mean(), inplace=True)
	data['Voltage'].fillna(data['Voltage'].mean(), inplace=True)
	data['Global_intensity'].fillna(data['Global_intensity'].mean(), inplace=True)
	
	# print info of dataset
	print ('dataset info:')
	data.info()
	print('\n')
	
	#datatypes were all float64 after fixing missing values
	# write preprocessed data to file
	data.to_csv('../preprocessed/' + targetFileName)
	
	# cleanup temporarily generated data
	cleanup(tmpDir)
	
#preprocess_data('household_power_consumption')