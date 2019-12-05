import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import count, sum, desc
from pyspark.sql.types import StringType


#######################################################################################################################

run_spark_in_cluster = False      # SET THIS VARIABLE FOR TESTING VS PRODUCTION

link_to_cluster_storage = "hdfs://namenode:9000"
link_to_local_storage = "../data/results"
if(run_spark_in_cluster):
    path_to_write = ""
else:
    path_to_write = link_to_local_storage

#######################################################################################################################


def toprated_movies_based_on_average_rating(df):
    df_most_popular = df.groupBy('MovieID')\
                        .agg(sum('Rating')/count('Rating'))\
                        .withColumnRenamed('(sum(Rating) / count(Rating))', 'avg_rating')\
                        .sort(desc('avg_rating'))

    df_most_popular\
        .coalesce(1)\
        .write\
        .format('com.databricks.spark.csv')\
        .mode('overwrite')\
        .save(path_to_write + "/task1", header='true', sep=',')

#######################################################################################################################


if run_spark_in_cluster:
    spark = SparkSession.builder.appName('hw4').master('spark://spark-master:7077')
else:
    spark = SparkSession.builder.appName('hw4').master('local')

spark = spark.getOrCreate()
sqlContext = SQLContext(spark)

# read in the data from dat files
rating_cols = ['UserID', 'MovieID', 'Rating', 'Timestamp']
user_cols = ['UserID', 'Gender', 'Age', 'Occupation', 'Zip-code']
movie_cols = ['MovieID', 'Title', 'Genres']

df_ratings = spark.sparkContext.textFile('../data/ratings.dat')\
    .map(lambda x: x.split("::"))\
    .toDF()\
    .toDF(*rating_cols)

df_users = spark.sparkContext.textFile('../data/users.dat')\
    .map(lambda x: x.split("::"))\
    .toDF()\
    .toDF(*user_cols)

df_movies = spark.sparkContext.textFile('../data/movies.dat')\
    .map(lambda x: x.split("::"))\
    .toDF()\
    .toDF(*movie_cols)

# print dataset information
# df_ratings.printSchema()
# df_users.printSchema()
# df_movies.printSchema()

# df_ratings.describe().show()
# df_users.describe().show()
# df_movies.describe().show()

# sub task 1: list the top-rated movies based on the ‘average’ rating score.
# (sorted in descending order of ‘average’ rating score)
toprated_movies_based_on_average_rating(df_ratings)

