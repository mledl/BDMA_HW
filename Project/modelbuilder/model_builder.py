from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler

conf = SparkConf().setAppName('localTest')
# conf = SparkConf().setAppName('app').setMaster('spark://spark-master:7077').setSparkHome('/spark/')
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.getOrCreate()

data = spark.read.csv('../data/train_transaction.csv', header=True, inferSchema=True)

data = data.drop('M1', "M2", "M3", "M4", "M5", "M6", "M7", "M8", "M9")

categoricalColumns = ["ProductCD",
                      "card1", "card2", "card3", "card4", "card5", "card6",
                      "addr1", "addr2",
                      "P_emaildomain", "R_emaildomain"]

stages = []  # stages in our Pipeline
for categoricalCol in categoricalColumns:
    # Category Indexing with StringIndexer
    stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index", handleInvalid="keep")
    # Use OneHotEncoder to convert categorical variables into binary SparseVectors
    # encoder = OneHotEncoderEstimator(inputCol=categoricalCol + "Index", outputCol=categoricalCol + "classVec")
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    # Add stages.  These are not run here, but will run all at once later on.
    stages += [stringIndexer, encoder]

label_stringIdx = StringIndexer(inputCol="isFraud", outputCol="label")
stages += [label_stringIdx]

numericColumns = [e for e in data.schema.names if e not in categoricalColumns + ["isFraud"]]

assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericColumns
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features", handleInvalid="keep")
stages += [assembler]

partialPipeline = Pipeline().setStages(stages)
pipelineModel = partialPipeline.fit(data)
preppedDataDF = pipelineModel.transform(data)

selectedcols = ["label", "features"] + data.columns
dataset = preppedDataDF.select(selectedcols)

# Train model with Training Data
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed=100)

# Create initial Decision Tree Model
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxDepth=3)

dtModel = dt.fit(trainingData)

print("numNodes = ", dtModel.numNodes)
print("depth = ", dtModel.depth)

print (dtModel)

# Make predictions on test data using the Transformer.transform() method.


predictions = dtModel.transform(testData)

predictions.printSchema()

print(predictions.head())

dtModel.save("model")
