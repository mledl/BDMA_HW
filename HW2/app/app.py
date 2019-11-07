import glob
import os
import findspark
import json

findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, IntegerType


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


def wordCountPerUnit(col, unit, df):
    # create new column 'counts' that holds an array of all words per row (date)
    df_timestamped_unit = df.withColumn('counts', F.struct(unit, F.split(F.col(col), ' ')))

    # make ((date, word), 1) pairs and count their specific occurrence
    rdd_count_per_unit = df_timestamped_unit.select('counts').rdd\
        .map(lambda r: [((r[0][0], w), 1) for w in r[0][1]])\
        .flatMap(lambda l: [x for x in l]).reduceByKey(lambda x, y: x + y)

    # transform tuples to (date, (word, count)) and combine all per date and sort the tuples within the specific date
    count_per_unit = rdd_count_per_unit.map(lambda r: (r[0][0], [(r[0][1], r[1])])).reduceByKey(lambda x, y: x + y)\
        .map(lambda t: (t[0], sorted(t[1], key=lambda x: -x[1])))
    return count_per_unit


def printWordcountPerKey(data, noOfElements, isDate):
    # data is in format [(key, [(w1, count1), ...]), ...]
    for t1 in data:
        i = 0
        if isDate == 1:
            print(t1[0].strftime('%Y/%m/%d'))
        else:
            print(t1[0])
        for t2 in t1[1]:
            print('<' + t2[0] + '> <' + str(t2[1]) + '>')
            i += 1
            if i > noOfElements:
                print('\n\n')
                break


def generateCooccurrenceMatrices(data, df_all):
    # specify schema of dataframe per topic
    schema = StructType([StructField("words", StringType()), StructField("count", IntegerType())])

    # to dataframe and create top 100 column
    df_temp = data.toDF(['Topic', 'Tuples per Topic'])
    all_tuples = df_temp.rdd.map(lambda r: (r[0], [w[0] for w in r[1]])).collect()
    df_temp = df_temp.withColumn('Top 100', F.slice('Tuples per Topic', start=1, length=100))
    count_tuples = df_temp.select(['Topic', 'Top 100']).rdd.map(lambda r: (r[0], [w[0] for w in r[1]])).collect()

    # list of all titles per topic
    titles_economy = df_all.select(['Title']).where(F.col('Topic').isin({'economy'})).rdd.flatMap(lambda r: r).collect()
    df_titles_economy = spark.createDataFrame(titles_economy, "string").toDF('Title')
    df_titles_economy = df_titles_economy.withColumn('Title', F.split(F.col('Title'), ' '))

    # lists of top100 words per topic
    economy_top100_words = count_tuples[0][1]
    microsoft_top100_words = count_tuples[1][1]
    palestine_top100_words = count_tuples[2][1]
    obama_top100_words = count_tuples[3][1]

    def mapStripes(sentence, top100_words):
        stripes = {}
        #i = 0
        for word in top100_words:
            h = {}
            for neigh in neighbors(word, sentence, top100_words):
                h[neigh] = h.get(neigh, 0) + 1
            stripes[word] = h
            #i += 1
            #if i == 5:
            #    print(sentence)
            #    print(json.dumps(stripes))
            #    break
        return stripes

    def neighbors(word, sentence, top100_words):
        relevant_words = removeIrrelevantWords(top100_words, sentence)
        return relevant_words if word in relevant_words else []

    def removeIrrelevantWords(top100_words, l):
        return list(set(l) & set(top100_words))

    def reduceStripes(word, stripes):
        result = {}
        print(stripes)
        for stripe in stripes:
            result = {k: result.get(k, 0) + stripe.get(k, 0) for k in set(result) | set(stripe)}
        return result

    # calculate stripes per title
    economy_coocurrence = df_titles_economy.rdd.map(lambda r: mapStripes(r[0], economy_top100_words))

    # combine

    print(json.dumps(economy_coocurrence))


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

# print config
noOfTuples = 20

# Note: it is important to excape the ", bc it is used in text and otherwise the file would be split up incorrectly
df_news = spark.read.csv(dir_path + "News_Final.csv", header=True, sep=",", escape='"',
                         timestampFormat='yyyy-MM-dd HH:mm:ss')

# analyse news_final dataset and print some stats
# printNullValuesPerColumn(df_news)
# printNanValuesPerColumn(df_news)

# fill null values in Headline with ''
df_news = df_news.fillna({'Headline': ''})

# parse the timestamp in order to make time windows
df_news = df_news.withColumn('PublishDate1', F.to_date('PublishDate', "yyyy-MM-dd HH:mm:ss"))
df_timestamped = df_news.select(['PublishDate1', 'Topic', 'Title', 'Headline'])

# drop duplicates
df_timestamped = df_timestamped.dropDuplicates(['Title', 'Headline'])


# count term frequency of title column and sort in descending order
# in total
title_counts_total = wordCountTotal(df_timestamped.select('Title').rdd)
headline_counts_total = wordCountTotal(df_timestamped.select('Headline').rdd)

# per day
title_counts_per_day = wordCountPerUnit('Title', 'PublishDate1', df_timestamped)
headline_counts_per_day = wordCountPerUnit('Headline', 'PublishDate1', df_timestamped)


print('###########################')
print('  Wordcount Title per Day')
print('###########################')
# printWordcountPerKey(title_counts_per_day.collect(), noOfTuples, 1)
print('###########################\n')

print('###########################')
print('Wordcount Headline per Day')
print('###########################')
# printWordcountPerKey(headline_counts_per_day.collect(), noOfTuples, 1)
print('###########################\n')

# per topic
title_counts_per_topic = wordCountPerUnit('Title', 'Topic', df_timestamped)
headline_counts_per_topic = wordCountPerUnit('Headline', 'Topic', df_timestamped)

print('#############################')
print('  Wordcount Title per Topic')
print('#############################')
# printWordcountPerKey(title_counts_per_topic.collect(), noOfTuples, 0)
print('###########################\n')

print('#############################')
print('Wordcount Headline per Topic')
print('#############################')
# printWordcountPerKey(headline_counts_per_topic.collect(), noOfTuples, 0)
print('###########################\n')

# co-occurrence matrices for Title and Headline for top 100 words per Topic
generateCooccurrenceMatrices(title_counts_per_topic, df_timestamped)


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

