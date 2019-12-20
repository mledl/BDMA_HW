import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import count, sum, desc, col, expr, lit, array
from sklearn.metrics.pairwise import cosine_similarity
from numpy import dot
from numpy.linalg import norm
import collections

#######################################################################################################################

run_spark_in_cluster = False      # SET THIS VARIABLE FOR TESTING VS PRODUCTION

k_user = 100
k_item = 20
movie_limit = 2
user = 1

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


def top_similar_users(df_r, user, k_user):
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
        .agg(expr("coalesce(first(norm_rating),0)").cast("double"))\
        .fillna(0)

    # create column of list of ratings per user
    df_user_movie_ratings_vec = df_user_movie_ratings\
        .withColumn('rate_vec', array([x for x in df_user_movie_ratings.columns if x != 'UserID']))\
        .select('UserID', 'rate_vec')

    # make df for the given user and one for all the others
    given_user_ratings = df_user_movie_ratings_vec.filter(df_user_movie_ratings.UserID == user).collect()[0]['rate_vec']
    df_other_users = df_user_movie_ratings_vec.filter(df_user_movie_ratings.UserID != user)

    # cosine similarity between dedicated user and all the others
    given_user_ratings_norm = norm(given_user_ratings)

    def cos_sim_user(userid, a):
        cos = dot(a, given_user_ratings) / (norm(a) * given_user_ratings_norm)
        return userid, cos.item()

    df_user_similarity = df_other_users.rdd.map(lambda r: cos_sim_user(r['UserID'], r['rate_vec']))\
        .toDF()\
        .toDF('UserID', 'similarity')\
        .sort(col('similarity').desc())

    # write to file
    df_user_similarity \
        .coalesce(1) \
        .write \
        .format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .save(path_to_write + "/task2", header='true', sep=',')

    return df_user_similarity.limit(k_user)


def top_similar_movies2(df_r, df_m, k_item, user, movie):
    # just take user into account that also rated the given movie
    #user_rated_movie = [int(r[0]) for r in df_r.where(col('MovieID') == movie).select('UserID').collect()]
    #df_r_data = df_r.where(col('UserID').isin(user_rated_movie))
    df_r_data = df_r

    # normalize the movie ratings by movie means
    df_movie_avg = df_r_data.groupBy('MovieID')\
        .agg(sum('Rating')/count('Rating'))\
        .withColumnRenamed('(sum(Rating) / count(Rating))', 'avg_rating')

    df_r = df_r_data.alias("df1").join(df_movie_avg.alias("df2"), df_r.MovieID == df_movie_avg.MovieID) \
        .select("df1.UserID", "df1.MovieID", "df1.Rating", "df2.avg_rating") \
        .withColumn("norm_rating", col('Rating') - col('avg_rating')) \
        .select("UserID", "MovieID", "norm_rating")

    # create pivot matrix MovieID/UserID with normalized ratings as values
    df_movie_user_ratings = df_r.groupBy("MovieID") \
        .pivot("UserID") \
        .agg(expr("coalesce(first(norm_rating),0)").cast("double"))\
        .fillna(0)

    df_movie_user_ratings.show(1)

    # make df for the given movie and one for all the others
    df_movie = df_movie_user_ratings.filter(df_movie_user_ratings.MovieID == movie)
    df_other_movies = df_movie_user_ratings.filter(df_movie_user_ratings.MovieID != movie)

    # cosine similarity between dedicated user and all the others
    pd_movie = df_movie.toPandas()
    pd_other_movies = df_other_movies.toPandas()
    cos_similarities = cosine_similarity(pd_movie, pd_other_movies)[0]
    cos_similarities = [float(cos_sim) for cos_sim in cos_similarities]
    pd_other_movies['similarity'] = [float(cos_sim) for cos_sim in cos_similarities]

    # create spark dataframe from pandas
    df_movie_similarity = spark.createDataFrame(pd_other_movies[['MovieID', 'similarity']])
    df_movie_similarity = df_movie_similarity.alias("df1").join(df_m.alias("df2"), df_movie_similarity.MovieID == df_m.MovieID)\
        .select(['df2.MovieID', 'df2.Title', 'similarity'])

    # sort descending by similarity
    df_movie_similarity = df_movie_similarity.sort(col('similarity').desc())

    # write to file
    df_movie_similarity\
        .select(['Title', 'similarity']) \
        .coalesce(1) \
        .write \
        .format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .save(path_to_write + "/task3", header='true', sep=',')

    # get similar movies that has also been rated by user
    movies_rated_by_user = [int(r[0]) for r in df_r.filter(col('UserID') == user).select(['MovieID']).collect()]
    df_sim_movies_rated_by_user = df_movie_similarity.filter(col('MovieID').isin(movies_rated_by_user))

    return df_sim_movies_rated_by_user.limit(k_item)


def top_similar_movies1(df_r, df_m, k_item, user, movie):
    # get items rated by user
    df_movies_rated_by_user = df_r.filter(col('UserID') == user)
    movies_rated_by_user = [int(row[0]) for row in df_movies_rated_by_user.select('MovieID').collect()]
    df_movies_unrated_by_user = df_m.filter(col('MovieID').isin(*movies_rated_by_user) == False)
    movies_unrated_by_user = [int(row[0]) for row in df_movies_unrated_by_user.select('MovieID').collect()]

    # normalize the movie ratings by movie means
    df_movie_avg = df_r.groupBy('MovieID') \
        .agg(sum('Rating') / count('Rating')) \
        .withColumnRenamed('(sum(Rating) / count(Rating))', 'avg_rating')

    df_r = df_r.alias("df1").join(df_movie_avg.alias("df2"), df_r.MovieID == df_movie_avg.MovieID) \
        .select("df1.UserID", "df1.MovieID", "df1.Rating", "df2.avg_rating") \
        .withColumn("norm_rating", col('Rating') - col('avg_rating')) \
        .select("UserID", "MovieID", "norm_rating")

    # create pivot matrix MovieID/UserID with normalized ratings as values
    df_movie_user_ratings = df_r.groupBy("MovieID") \
        .pivot("UserID") \
        .agg(expr("coalesce(first(norm_rating),0)").cast("double")) \
        .fillna(0)

    # create column of list of ratings per movie
    df_movie_user_ratings_vec = df_movie_user_ratings \
        .withColumn('rate_vec', array([x for x in df_movie_user_ratings.columns if x != 'MovieID'])) \
        .select('MovieID', 'rate_vec')

    # split items in two sets, rated and unrated by given user
    df_movies_rated_by_user = df_movie_user_ratings_vec.filter(col('MovieID').isin(*movies_rated_by_user))
    df_movies_unrated_by_user = df_movie_user_ratings_vec.filter(col('MovieID').isin(*movies_unrated_by_user))
    print('movies rated: ' + str(df_movies_rated_by_user.count()))
    print('movies unrated: ' + str(df_movies_unrated_by_user.count()))

    # optimize similarity calculation by focusing on co rated movies
    #def co_rated_movies(movieid):
    #    user_rated_movie = [int(row[0]) for row in df_r.filter(col('MovieID') == movieid).select('UserID').collect()]
    #    co_rated_movies_list = [int(row['MovieID']) for row in df_r.filter(col('UserID').isin(*user_rated_movie)).collect()]
    #    co_rated_movies_list = list(dict.fromkeys(co_rated_movies_list))
    #    if movieid in co_rated_movies_list:
    #        co_rated_movies_list.remove(movieid)
    #    print(movieid)
    #    print(user_rated_movie)
    #    print(co_rated_movies_list)
    #    return df_movies_rated_by_user.filter(df_movie_user_ratings_vec['MovieID'].isin(*co_rated_movies_list))

    # modified cosine similarity function to use with map
    def cos_sim_item(movieid, a, b, b_norm):
        a_norm = norm(a)
        if a_norm == 0:
            return movieid, 0
        cos = dot(a, b) / (a_norm * b_norm)
        return movieid, cos.item()

    # calculate similarity to every rated movie for every unrated movie by given user
    item_similarities_dict = collections.defaultdict()
    for row in df_movies_unrated_by_user.collect():
        #df_co_rated = co_rated_movies(row['MovieID'])
        given_movie_norm = norm(row['rate_vec'])
        print(given_movie_norm)
        if given_movie_norm != 0:
            item_similarities_dict[row['MovieID']] = df_movies_rated_by_user\
                .rdd\
                .map(lambda r: cos_sim_item(r['MovieID'], r['rate_vec'], row['rate_vec'], given_movie_norm))\
                .collect()
            print(item_similarities_dict)


def get_normalized_movie_df(df_r):
    # normalize the movie ratings by movie means
    df_movie_avg = df_r.groupBy('MovieID') \
        .agg(sum('Rating') / count('Rating')) \
        .withColumnRenamed('(sum(Rating) / count(Rating))', 'avg_rating')

    df_r = df_r.alias("df1").join(df_movie_avg.alias("df2"), df_r.MovieID == df_movie_avg.MovieID) \
        .select("df1.UserID", "df1.MovieID", "df1.Rating", "df2.avg_rating") \
        .withColumn("norm_rating", col('Rating') - col('avg_rating')) \
        .select("UserID", "MovieID", "norm_rating")

    # create pivot matrix MovieID/UserID with normalized ratings as values
    df_movie_user_ratings = df_r.groupBy("MovieID") \
        .pivot("UserID") \
        .agg(expr("coalesce(first(norm_rating),0)").cast("double")) \
        .fillna(0)

    # create column of list of ratings per user
    df_movie_user_ratings_vec = df_movie_user_ratings \
        .withColumn('rate_vec', array([x for x in df_movie_user_ratings.columns if x != 'MovieID'])) \
        .select('MovieID', 'rate_vec')

    return df_movie_user_ratings_vec


def top_similar_movies(df_r, df_norm_vec, k_item, user, movie, to_file):
    # extract movies rated by given user
    df_movies_rated_by_user = df_r.filter(col('UserID') == user)
    movies_rated_by_user = [int(row[0]) for row in df_movies_rated_by_user.select('MovieID').collect()]

    # create dataframe that holds the movies that were already rated by the user
    df_movies_rated_by_user = df_norm_vec.filter(col('MovieID').isin(*movies_rated_by_user))

    # extract the movie we want to calculate the similarity for
    given_movie_ratings = df_norm_vec.filter(df_norm_vec.MovieID == movie).collect()[0]['rate_vec']

    # calculate norm of movie to calculate similarity for only once
    given_movie_ratings_norm = norm(given_movie_ratings)

    # cosine similarity between given movie and all other movies also rated by given user
    def cos_sim_item(movieid, a):
        cos = dot(a, given_movie_ratings) / (norm(a) * given_movie_ratings_norm)
        return movieid, cos.item()

    df_movie_similarity = df_movies_rated_by_user.rdd.map(lambda r: cos_sim_item(r['MovieID'], r['rate_vec']))\
        .toDF()\
        .toDF('MovieID', 'similarity')\
        .sort(col('similarity').desc())

    # write to file
    if to_file:
        df_movie_similarity \
            .coalesce(1) \
            .write \
            .format('com.databricks.spark.csv') \
            .mode('overwrite') \
            .save(path_to_write + "/task3", header='true', sep=',')

    return df_movie_similarity.limit(k_item)


def user_user_based_recommendation(df_k_similar_user, df_r, df_m):
    # calculate sum of all weights and add as column
    sum_weights = df_k_similar_user.select(sum(col('similarity'))).collect()[0][0]
    df_k_similar_user = df_k_similar_user.withColumn('sum_weights', lit(sum_weights))

    df_k_similar_user.show()

    # join the movie ratings
    df_user_movie_ratings = df_k_similar_user.alias("df1").join(df_r, df_k_similar_user.UserID == df_r.UserID)\
        .select('df1.UserID', 'MovieID', 'Rating', 'similarity', 'sum_weights')

    # calculate sim(user, user_n) * rating(user_n, movie) by grouping and aggregating
    df_user_movie_pred_ratings = df_user_movie_ratings.groupby('MovieID')\
        .agg(sum((col('similarity') * col('Rating'))/col('sum_weights')))\
        .withColumnRenamed('sum(((similarity * Rating) / sum_weights))', 'prediction')

    # join with movies to get the titles instead of movie id
    df_user_movie_pred_ratings = df_user_movie_pred_ratings\
        .join(df_m, df_user_movie_pred_ratings.MovieID == df_m.MovieID)\
        .select(['Title', 'prediction'])\
        .sort(desc('prediction'))

    df_user_movie_pred_ratings.show()

    # write to file
    df_user_movie_pred_ratings \
        .coalesce(1) \
        .write \
        .format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .save(path_to_write + "/task4b", header='true', sep=',')


def item_item_based_recommendation(df_r, df_m, df_norm_vec, user, movie_limit):
    # get a list of movies the given user has not rated yet
    df_movies_rated_by_user = df_r.filter(col('UserID') == user)
    movies_rated_by_user = [int(row[0]) for row in df_movies_rated_by_user.select('MovieID').collect()]
    df_movies_unrated_by_user = df_m.filter(col('MovieID').isin(*movies_rated_by_user) == False)
    movies_unrated_by_user = [int(row[0]) for row in df_movies_unrated_by_user.select('MovieID').collect()]
    print('user: ' + str(user) + ' has ' + str(len(movies_unrated_by_user)) + ' unrated movies!')

    # take specified number of unrated movies of given user
    movies_to_predict = movies_unrated_by_user[0:movie_limit]

    # iterate over given unrated movies
    predictions = []
    for movieid in movies_to_predict:
        # calculate k similar movies to given iteration movie
        df_k_similar_movies = top_similar_movies(df_r, df_norm_vec, k_item, user, movieid, False)

        # join ratings of given user to movie similarities
        df_k_similar_movies = df_k_similar_movies.alias("df1")\
            .join(df_movies_rated_by_user, df_k_similar_movies.MovieID == df_movies_rated_by_user.MovieID)\
            .select(['UserID', 'df1.MovieID', 'Rating', 'similarity'])

        # calculate the predicted value for iteration movie
        movieid_prediction = df_k_similar_movies\
            .groupby('UserID')\
            .agg(sum(col('similarity') * col('Rating')) / sum('similarity'))\
            .collect()

        predictions.append((movieid, movieid_prediction[0][1]))

    # create dataframe from predictions
    df_movie_recommendations = spark.createDataFrame(predictions).toDF('MovieID', 'prediction')

    # joint movie dataframe to get title of movies and sort in descending prediction order
    df_movie_recommendations = df_movie_recommendations\
        .join(df_m, df_movie_recommendations.MovieID == df_m.MovieID)\
        .select(['Title', 'prediction'])\
        .sort(col('prediction').desc())

    # write the movie predictions to file
    df_movie_recommendations \
        .coalesce(1) \
        .write \
        .format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .save(path_to_write + "/task4b", header='true', sep=',')

#########################################################################################################


if run_spark_in_cluster:
    spark = SparkSession.builder.appName('hw4').master('spark://spark-master:7077')
else:
    spark = SparkSession.builder.appName('hw4').master('local')

spark = spark.getOrCreate()
sqlContext = SQLContext(spark)

# read in the data from dat files and just take needed columns
rating_cols = ['UserID', 'MovieID', 'Rating', 'Timestamp']
movie_cols = ['MovieID', 'Title', 'Genres']

df_ratings = spark.sparkContext.textFile('../data/ratings.dat')\
    .map(lambda x: x.split("::"))\
    .toDF()\
    .toDF(*rating_cols)\
    .select(['UserID', 'MovieID', 'Rating'])

df_ratings_5user = spark.sparkContext.textFile('../data/ratings_5user.dat')\
    .map(lambda x: x.split("::"))\
    .toDF()\
    .toDF(*rating_cols)\
    .select(['UserID', 'MovieID', 'Rating'])

df_movies = spark.sparkContext.textFile('../data/movies.dat')\
    .map(lambda x: x.split("::"))\
    .toDF()\
    .toDF(*movie_cols)\
    .select(['MovieID', 'Title'])

# explore some dataset stats
#total_number_movies = df_movies.count()
#total_number_user_ratings = df_ratings.count()
#number_unique_movies = df_movies.agg(countDistinct('MovieID')).collect()[0][0]
#number_unique_users = df_ratings.agg(countDistinct('UserID')).collect()[0][0]

#print('Total number of movies: ' + str(total_number_movies))
#print('Number of unique movies: ' + str(number_unique_movies))
#print('Total number of user ratings: ' + str(total_number_user_ratings))
#print('Number of unique users: ' + str(number_unique_users))

# calculate the number of ratings per movie
#df_ratings_per_movie = df_ratings.groupBy(col('MovieID')).agg(count(col('Rating')).alias('number_ratings'))
#df_ratings_per_movie.show()

# calculate the number of ratings a user did
#df_ratings_per_user = df_ratings.groupBy(col('UserID')).agg(count(col('Rating')).alias('number_ratings'))
#df_ratings_per_user.show()

# create table having rating and movie information
#df_user_movie_ratings = df_ratings.join(df_movies, df_ratings.MovieID == df_movies.MovieID)
#df_user_movie_ratings.show()

# sub task 1: list the top-rated movies based on the ‘average’ rating score.
# (sorted in descending order of ‘average’ rating score)
#toprated_movies_based_on_average_rating(df_ratings)

# sub task 2: Given any user, please list the top-’similar’ users based on
# the cosine similarity of previous ratings each user has given.
# (sorted in descending order of ‘user’ similarity score)
# calculate similarities to user k and store data to csv in format <user, similarity>
#df_top_sim_users = top_similar_users(df_ratings, user, k_user)

# sub task 3: Given any movie, please list the top-’similar’ movies based on
# the cosine similarity of previous ratings each movie received.
# (sorted in descending order of ‘item’ similarity score)
# calculate similarities to movie k and store data to csv in format <movie, similarity>
# get movie that has not been rated by user
#movie = 1
#df_norm_movie_pivot = get_normalized_movie_df(df_ratings)
#df_movie_sim_to_movie1 = top_similar_movies2(df_ratings, df_norm_movie_pivot, k_item, user, movie, True)

# sub task 4: Please implement a recommender system that recommends movies for a given user based on collaborative
# filtering: item-based, and user-based. (sorted in descending order of similarity score)
# (a) For item-based collaborative filtering: estimated by similar items
# (b) For user-based collaborative filtering: estimated by similar users
df_top_sim_users = spark.read.csv('../data/sim_users.csv', header=True, sep=",").toDF('UserID', 'similarity').limit(k_user)
user_user_based_recommendation(df_top_sim_users, df_ratings, df_movies)

#df_top_sim_movies = spark.read.csv('../data/sim_movies.csv', header=True, sep=",").toDF('Title', 'similarity')
#movies_rated_by_user = [int(r[0]) for r in df_ratings.filter(col('UserID') == user).select(['MovieID']).collect()]
#df_top_sim_movies = df_top_sim_movies.join(df_movies.alias("df2"), df_top_sim_movies.Title == df_movies.Title)\
#    .select(['MovieID', 'df2.Title', 'similarity'])
#top_sim_movies = [int(r[0]) for r in df_top_sim_movies.select(['MovieID']).collect()]
#inter_movies = list(set(top_sim_movies) & set(movies_rated_by_user))
#df_top_sim_movies = df_top_sim_movies.filter(col('MovieID').isin(movies_rated_by_user))\
#    .sort(col('similarity').desc())\
#    .limit(k_item)
#df_top_sim_movies.show()
#item_item_based_recommendation(df_ratings, df_movies, df_norm_movie_pivot, user, movie_limit)
