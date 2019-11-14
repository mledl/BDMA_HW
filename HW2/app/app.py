from collections import defaultdict
import findspark
import pandas as pd

findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StopWordsRemover, Tokenizer

data_dir = "../data/"
hdfs_dir = "hdfs://namenode:9000/data/"
dir_path = data_dir


def printNullValuesPerColumn(df):
    print('Number of Null values per column:')
    df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()


def printNanValuesPerColumn(df):
    print('Number of NaN values per column:')
    df.select([F.count(F.when(F.isnan(c), c)).alias(c) for c in df.columns]).show()


def wordCountTotal(rddCol, col):
    # calculate word counts in total
    counts = rddCol.flatMap(lambda r: r[0]).\
        map(lambda w: (w, 1)).\
        reduceByKey(lambda x, y: x + y).\
        sortBy(lambda wc: wc[1], False).toDF(['Word', 'Count'])

    # write wordcounts to file
    counts \
        .repartition(1) \
        .write \
        .mode("overwrite") \
        .csv(dir_path + "results/" + "wordcount_" + col + "_total", header=True)

    return counts.collect()


def wordCountPerUnit(col, unit, df_unit):
    # make ((date, word), 1) pairs and count their specific occurrence
    rdd_count_per_unit = df_unit.select(unit, col).rdd \
        .map(lambda r: [((r[0], w), 1) for w in r[1]]) \
        .flatMap(lambda l: [x for x in l]).reduceByKey(lambda x, y: x + y)

    # transform tuples to (date, (word, count)) and combine all per date and sort the tuples within the specific date
    count_per_unit = rdd_count_per_unit.map(lambda r: (r[0][0], [(r[0][1], r[1])])).reduceByKey(lambda x, y: x + y) \
        .map(lambda t: (t[0], sorted(t[1], key=lambda x: -x[1])))

    # format data to write sorted list to file
    print_count_per_unit = count_per_unit.toDF([unit, 'Wordcount']).\
        withColumn('Wordcount', F.explode(F.col('Wordcount'))).select(unit, 'Wordcount.*').\
        toDF(unit, 'Word', 'Count')

    # actually create file per group of lists
    print_count_per_unit \
        .repartition(1) \
        .write \
        .mode("overwrite") \
        .csv(dir_path + "results/" + "wordcount_" + col + "_per_" + unit, header=True)

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
    # to dataframe and create top 100 column
    df_temp = data.toDF(['Topic', 'Tuples per Topic'])
    df_temp = df_temp.withColumn('Top 100', F.slice('Tuples per Topic', start=1, length=100))
    count_tuples = df_temp.select(['Topic', 'Top 100']).rdd.map(lambda r: (r[0], [w[0] for w in r[1]])).collect()

    # lists of top100 words per topic
    economy_top100_words = count_tuples[0][1]
    microsoft_top100_words = count_tuples[1][1]
    palestine_top100_words = count_tuples[2][1]
    obama_top100_words = count_tuples[3][1]

    def mapOcc(sentence, top100_words):
        data_combs = []
        for word in top100_words:
            h = {}
            if word in sentence:
                for neigh in neighbors(sentence, top100_words, word):
                    comb = (word, neigh)
                    h[comb] = h.get(comb, 0) + 1
                h[(word, word)] = 0
                data_combs.extend(list(h.items()))
        return data_combs

    def neighbors(sentence, top100_words, word):
        neighs = list(set(sentence) & set(top100_words))
        neighs.remove(word)
        return neighs

    def generateCoocurrenceMatrix(col, topic, top100_words):
        # list of all titles per topic
        col_topic = df_all.select(col + '_sentence').where(F.col('Topic').isin({topic})).rdd.flatMap(
            lambda r: r).collect()
        df_col_topic = spark.createDataFrame(col_topic, "string").toDF(col)
        df_col_topic = df_col_topic.withColumn(col, F.split(F.col(col), ' '))

        # calculate co-occurrence stripes per title
        economy_coocurrence = df_col_topic.rdd. \
            flatMap(lambda r: mapOcc(r[0], top100_words)). \
            reduceByKey(lambda x, y: x + y). \
            map(lambda x: (x[0][0], [(x[0][1], int(x[1]))])). \
            reduceByKey(lambda x, y: x + y).sortByKey().collect()

        # construct matrix from coocurrence stripes
        as_matrix = defaultdict(dict)
        for entry in economy_coocurrence:
            topword = entry[0]
            for cooc in sorted(entry[1]):
                as_matrix[topword][cooc[0]] = cooc[1]

        # use pandas to create a dataframe from matrix
        pd_df_matrix = pd.DataFrame(as_matrix)
        pd_df_matrix.insert(0, 'words', sorted(top100_words), True)
        df_matrix = spark.createDataFrame(pd_df_matrix)
        df_matrix = df_matrix.na.fill(0)

        # write matrix to file
        df_matrix \
            .repartition(1) \
            .write \
            .mode("overwrite") \
            .csv(dir_path + "results/" + "cooc_matrix_" + col + '_' + topic, header=True)

    generateCoocurrenceMatrix('Title', 'economy', economy_top100_words)
    generateCoocurrenceMatrix('Title', 'microsoft', microsoft_top100_words)
    generateCoocurrenceMatrix('Title', 'palestine', palestine_top100_words)
    generateCoocurrenceMatrix('Title', 'obama', obama_top100_words)
    generateCoocurrenceMatrix('Headline', 'economy', economy_top100_words)
    generateCoocurrenceMatrix('Headline', 'microsoft', microsoft_top100_words)
    generateCoocurrenceMatrix('Headline', 'palestine', palestine_top100_words)
    generateCoocurrenceMatrix('Headline', 'obama', obama_top100_words)


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

# fill null values in Headline with ''
df_news = df_news.fillna({'Headline': ''})

# parse the timestamp in order to make time windows
df_news = df_news.withColumn('PublishDate1', F.to_date('PublishDate', "yyyy-MM-dd HH:mm:ss"))
df_timestamped = df_news.select(['PublishDate1', 'Topic', 'Title', 'Headline'])

# drop duplicates
#df_timestamped = df_timestamped.dropDuplicates(['Title', 'Headline'])

# remove punctuation from text data, text to lower case and trim whitespaces
df_timestamped = df_timestamped.withColumn('Title', F.trim(F.lower(
    F.regexp_replace(F.col('Title'), '[^\sa-zA-Z0-9]', ''))))
df_timestamped = df_timestamped.withColumn('Headline', F.trim(F.lower(
    F.regexp_replace(F.col('Headline'), '[^\sa-zA-Z0-9]', ''))))

# tokenize titles and headlines
title_tokenizer = Tokenizer(inputCol='Title', outputCol='Title_words')
headline_tokenizer = Tokenizer(inputCol='Headline', outputCol='Headline_words')
df_timestamped = title_tokenizer.transform(df_timestamped)
df_timestamped = headline_tokenizer.transform(df_timestamped)

# remove stop words
titel_remover = StopWordsRemover(inputCol='Title_words', outputCol='Title_final')
headline_remover = StopWordsRemover(inputCol='Headline_words', outputCol='Headline_final')
df_timestamped = titel_remover.transform(df_timestamped)
df_timestamped = headline_remover.transform(df_timestamped)

# simplify dataframe
df_timestamped = df_timestamped.select(F.col('PublishDate1').alias('PublishDate'),
                                       F.col('Topic'),
                                       F.col('Title_final').alias('Title'),
                                       F.col('Headline_final').alias('Headline'),
                                       F.col('Title').alias('Title_sentence'),
                                       F.col('Headline').alias('Headline_sentence'))

# count term frequency of title column and sort in descending order
# in total
title_counts_total = wordCountTotal(df_timestamped.select('Title').rdd, 'Title')
headline_counts_total = wordCountTotal(df_timestamped.select('Headline').rdd, 'Headline')

# per day
title_counts_per_day = wordCountPerUnit('Title', 'PublishDate', df_timestamped)
headline_counts_per_day = wordCountPerUnit('Headline', 'PublishDate', df_timestamped)

# per topic
title_counts_per_topic = wordCountPerUnit('Title', 'Topic', df_timestamped)
headline_counts_per_topic = wordCountPerUnit('Headline', 'Topic', df_timestamped)

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
        .csv(dir_path + "results/" + platform + '_' + str(number_of_col), header=True)


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
