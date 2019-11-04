import glob
import os
import findspark

findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

data_dir = "../data/"
hdfs_dir = "hdfs://namenode:9000/data/"
dir_path = hdfs_dir


def printNullValuesPerColumn(df):
    print('Number of Null values per column:')
    df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()


def printNanValuesPerColumn(df):
    print('Number of NaN values per column:')
    df.select([F.count(F.when(F.isnan(c), c)).alias(c) for c in df.columns]).show()


def wordCountTotal(rddCol):
    words = rddCol.map(lambda r: r[0]).flatMap(lambda x: x.split(' '))
    counts = words.map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda wc: wc[1], False)
    return counts.collect()


def cleanup(dir, del_dir):
    # delete tmp directory and file(s)
    tmpFileList = [f for f in os.listdir(dir)]
    for f in tmpFileList:
        os.remove(os.path.join(dir, f))

    if del_dir == 1:
        os.rmdir(dir)

    print('successfully deleted directory: ' + dir)


def print_statistics(stats, norm_data):
    df_print = spark.createDataFrame(stats, ['column_name', 'count', 'min', 'max', 'mean', 'std'])
    df_print.show()

    if os.path.exists('../results'):
        cleanup('../results', 0)

    df_print.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode('overwrite').save(
        "../results/tmp_stats")
    for fileName in glob.glob('../results/tmp_stats/part-0000*.csv'):
        os.rename(fileName, '../results/hw1_stats.csv')
        print('successfully created ../results/hw1_stats.csv')

    # cleanup("../results/tmp_stats", 1)

    new_col_names = ['Norm_global_active_power', 'Norm_global_reactive_power', 'Norm_voltage', 'Norm_global_intensity'];
    norm_data_print = norm_data.toDF(*new_col_names)
    norm_data_print.write.format("com.databricks.spark.csv").option("header", "true").mode('append').save(
        "../results/tmp_norm")
    merge_files('../results/tmp_norm/part-0*.csv')
    # cleanup('../results/tmp_norm', 1)


def merge_files(files):
    first = 1
    with open('../results/hw1_min_max_normalization.csv', 'a') as targetFile:
        print(glob.glob(files))
        for fileName in glob.glob(files):
            with open(fileName) as sourceFile:
                m = sourceFile.readlines()
                if first == 1:
                    lines = m[0:]
                    first = 0
                else:
                    lines = m[1:]

                for line in lines:
                    targetFile.write(line.rstrip() + '\n')

    print('successfully merged data into file: ../results/hw1_min_max_normalization.csv')


# Spark setup
# for local testing
conf = SparkConf().setAppName('localTest')
# conf = SparkConf().setAppName('app').setMaster('spark://spark-master:7077').setSparkHome('/spark/')
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.getOrCreate()

df_news = spark.read.csv(dir_path + "News_Final.csv", header=True, sep=",", escape='"',
                         timestampFormat='yyyy-MM-dd HH:mm:ss')

# analyse news_final dataset and print some stats
# printNullValuesPerColumn(df_news)
# printNanValuesPerColumn(df_news)

# fill null values in Headline with ''
df_news = df_news.fillna({'Headline': ''})

# parse the timestamp in order to make time windows
df_news = df_news.withColumn('PublishDate1', F.to_date('PublishDate', "yyyy-MM-dd HH:mm:ss"))
df_timestamped = df_news.select(['PublishDate1', 'Title', 'Headline'])


# count term frequency of title column and sort in descending order
# in total
# title_counts_total = wordCountTotal(df_timestamped.select('Title').rdd) HERE
# headline_counts_total = wordCountTotal(df_timestamped.select('Headline').rdd)
#
# # per day
# # create timestamp word pairs
#
#
# # group per day
#
# # df_timestamped.show()
# # group by timestamp and concatenate strings using aggregation, then split into the single words
# df_timestamped_day = df_timestamped.groupBy('PublishDate1')\
#                                     .agg(F.split(F.concat_ws(' ', F.collect_list('Title')), ' ').alias('agg_title'),
#                                          F.split(F.concat_ws(' ', F.collect_list('Headline')), ' ').alias('agg_headline'))\
#                                     .sort('PublishDate1')
#
# # df_timestamped_day = df_timestamped_day.withColumn('wc_pair_title',)
#
# test = df_timestamped_day.select('agg_title').rdd.map(lambda l: wordCountTotal(sc.parallelize(l))).collect()
#
# print(test[0:20]) HERE

# df_timestamped_day.show()

# words_per_day = df_timestamped.rdd.map(lambda )


# output = title_counts_total
# i = 1
# for (word, count) in output:
#    print("%s: %i" % (word, count))
#    if i == 10:
#        break
#    i += 1


# TASK 2 In social feedback data, calculate the average popularity of each news by hour, and by day, respectively (for each platform)
# By hour requires to calculate average on every 3 columns, by day requires to calculate average on every 72 columns
def calculate_average_popularity(platform, number_of_col):
    economy = spark.read.csv(dir_path + platform + "_Economy.csv", header=True, sep=",", escape='"')
    microsoft = spark.read.csv(dir_path + platform + "_Microsoft.csv", header=True, sep=",", escape='"')
    obama = spark.read.csv(dir_path + platform + "_Obama.csv", header=True, sep=",", escape='"')
    palestine = spark.read.csv(dir_path + platform + "_Palestine.csv", header=True, sep=",", escape='"')
    topics_data = economy.union(microsoft).union(obama).union(palestine)

    def calculate_columns(start, skip):
        columns = []
        for i in range(start, start + skip):
            columns.append('TS' + str(i))
        return columns

    def calculate_average(data, columns):
        sum = 0
        for i in columns:
            sum += data[i]
        return sum / len(columns)

    for index, i in enumerate(range(1, 144, number_of_col)):
        columns = calculate_columns(i, number_of_col)

        topics_data = topics_data.withColumn('slot' + str(index), calculate_average(topics_data, columns))
        topics_data = topics_data.drop(*columns)

    topics_data \
        .repartition(1) \
        .write \
        .mode("overwrite") \
        .csv(dir_path + "results/" + platform + '_' + str(number_of_col) + '.csv', header=True)

# TASK_3: In news data, calculate the sum and average sentiment score of each topic, respectively
def calculate_sum_average_sentiment_score_by_topic(news):
    news = news.withColumn('sentiment', news.SentimentTitle + news.SentimentHeadline)
    sum = news.groupBy('Topic').sum("sentiment")
    average = news.groupBy('Topic').avg("sentiment")
    return sum.join(average, on=['Topic'])


calculate_average_popularity("Facebook", 3)
calculate_average_popularity("Facebook", 72)
calculate_average_popularity("GooglePlus", 3)
calculate_average_popularity("GooglePlus", 72)
calculate_average_popularity("LinkedIn", 3)
calculate_average_popularity("LinkedIn", 72)

calculate_sum_average_sentiment_score_by_topic(df_news) \
    .show()

