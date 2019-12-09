import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import count, sum, desc, col, expr
from sklearn.metrics.pairwise import cosine_similarity
import databricks.koalas as ks
import numpy as np


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


def top_similar_users_old(df_r):
    # normalize the user ratings by user means
    df_user_avg = df_r.groupBy('UserID')\
                        .agg(sum('Rating')/count('Rating'))\
                        .withColumnRenamed('(sum(Rating) / count(Rating))', 'avg_rating')

    df_r = df_r.alias("df1").join(df_user_avg.alias("df2"), df_r.UserID == df_user_avg.UserID)\
        .select("df1.UserID", "df1.MovieID", "df1.Rating", "df2.avg_rating")\
        .withColumn("norm_rating", col('Rating') - col('avg_rating'))\
        .select("UserID", "MovieID", "norm_rating")

    # create pivot matrix UserID/MovieID with normalized ratings as values
    ks_user_movie = ks.DataFrame(df_r).pivot_table(values='norm_rating', index=['UserID'],
                                                   columns='MovieID')
    # replacing NaN by movie avg
    ks_movie = ks_user_movie.fillna(ks_user_movie.mean(axis=0))

    # replacing NaN by user avg
    ks_user = ks_user_movie.applymap(lambda r: r.fillna(r.mean()))

    # calculate the user similarity
    cosine_sim = cosine_similarity(ks_user_movie)
    np.fill_diagonal(cosine_sim, 0)
    user_similarity = ks.DataFrame(cosine_sim, index=ks_user_movie.index)
    user_similarity.columns = ks_user.index

    print(user_similarity)


def top_similar_users_pandas(df_r, user):
    # normalize the user ratings by user means
    df_user_avg = df_r.groupBy('UserID') \
        .agg(sum('Rating') / count('Rating')) \
        .withColumnRenamed('(sum(Rating) / count(Rating))', 'avg_rating')

    df_r = df_r.alias("df1").join(df_user_avg.alias("df2"), df_r.UserID == df_user_avg.UserID) \
        .select("df1.UserID", "df1.MovieID", "df1.Rating", "df2.avg_rating") \
        .withColumn("norm_rating", col('Rating') - col('avg_rating')) \
        .select("UserID", "MovieID", "norm_rating")

    # create pivot matrix UserID/MovieID with normalized ratings as values
    # fill with 0 because it is faster
    df_user_movie_ratings = df_r.groupBy("UserID")\
        .pivot("MovieID")\
        .agg(expr("coalesce(first(norm_rating),0)").cast("double")).fillna(0)

    # make df for the given user and one for all the others
    df_user = df_user_movie_ratings.filter(df_user_movie_ratings.UserID == user)
    df_other_users = df_user_movie_ratings.filter(df_user_movie_ratings.UserID != user)

    # cosine similarity between dedicated user and all the others
    pd_user = df_user.toPandas()
    pd_other_users = df_other_users.toPandas()
    cos_similarities = cosine_similarity(pd_user, pd_other_users)[0]
    cos_similarities = [float(cos_sim) for cos_sim in cos_similarities]

    # create output format
    user_similarity = dict(zip(pd_other_users['UserID'], cos_similarities))

    # sort descending by similarity
    user_similarity = sorted(user_similarity.items(), key=lambda x: x[1])
    user_similarity.reverse()

    # make df and write to file
    df_user_similarity = sqlContext.createDataFrame(user_similarity, ['user', 'similarity'])
    df_user_similarity.show()
    df_user_similarity \
        .coalesce(1) \
        .write \
        .format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .save(path_to_write + "/task2", header='true', sep=',')


def top_similar_movies(df_r, movie):
    # normalize the movie ratings by movie means
    df_movie_avg = df_r.groupBy('MovieID')\
        .agg(sum('Rating')/count('Rating'))\
        .withColumnRenamed('(sum(Rating) / count(Rating))', 'avg_rating')

    df_r = df_r.alias("df1").join(df_movie_avg.alias("df2"), df_r.MovieID == df_movie_avg.MovieID) \
        .select("df1.UserID", "df1.MovieID", "df1.Rating", "df2.avg_rating") \
        .withColumn("norm_rating", col('Rating') - col('avg_rating')) \
        .select("UserID", "MovieID", "norm_rating")







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

df_ratings_5user = spark.sparkContext.textFile('../data/ratings_5user.dat')\
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
#toprated_movies_based_on_average_rating(df_ratings)

# sub task 2: Given any user, please list the top-’similar’ users based on
# the cosine similarity of previous ratings each user has given.
# (sorted in descending order of ‘user’ similarity score)
# calculate similarities to user #1 and store data to csv in format <user, similarity>
top_similar_users_pandas(df_ratings, 1)


# sub task 3: Given any movie, please list the top-’similar’ movies based on
# the cosine similarity of previous ratings each movie received.
# (sorted in descending order of ‘item’ similarity score)
#top_similar_movies(df_ratings, 2000)


