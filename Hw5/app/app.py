import findspark

findspark.init()

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import count, col
from pyspark.sql.types import StringType

#######################################################################################################################

run_spark_in_cluster = False  # SET THIS VARIABLE FOR TESTING VS PRODUCTION

dataset_path = "../data/web-Google.txt"
dataset_path_mini = "../data/web-Google-mini.txt"

task3_node = 1
no_of_updates = 15
weight = 0.85
offset = 0.15

link_to_cluster_storage = "hdfs://namenode:9000"
link_to_local_storage = "../data/results"
if (run_spark_in_cluster):
    path_to_write = ""
else:
    path_to_write = link_to_local_storage


#######################################################################################################################
def web_pages_sorted_by_outlinks(df_l):
    # get number of out-links per from node
    df_links_per_from_node = df_l \
        .groupby('FromNodeId') \
        .agg(count(col('ToNodeId'))) \
        .withColumnRenamed('count(ToNodeId)', 'out_degree') \
        .sort(col('out_degree').desc())

    # write the list to file
    df_links_per_from_node \
        .coalesce(1) \
        .write \
        .format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .save(path_to_write + "/task1", header='true', sep=',')


def web_pages_sorted_by_inlinks(df_l):
    # get number of in-links per to node
    df_links_per_to_node = df_l \
        .groupby('ToNodeId') \
        .agg(count(col('FromNodeId'))) \
        .withColumnRenamed('count(FromNodeId)', 'in_degree') \
        .sort(col('in_degree').desc())

    # write the list to file
    df_links_per_to_node \
        .coalesce(1) \
        .write \
        .format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .save(path_to_write + "/task2", header='true', sep=',')


def node_connectivity(df_l, node):
    # extract out-links from given node
    node_out_links = df_l\
        .filter(col('FromNodeId') == node)\
        .select('ToNodeId')\
        .collect()

    # extract in-links from given node
    node_in_links = df_l\
        .filter(col('ToNodeId') == node)\
        .select('FromNodeId')\
        .collect()

    # extract the numbers from rows
    node_out_links = [str(r[0]) for r in node_out_links]
    node_in_links = [str(r[0]) for r in node_in_links]
    node_out_in_links = [('v - ToNodeIds', node_out_links), ('FromNodeIds - v', node_in_links)]

    # create a dataframe holding those lists
    df_node_out_in_links = spark.sparkContext.parallelize(node_out_in_links).toDF().toDF('Description', 'Nodes')

    # write the lists to file
    df_node_out_in_links \
        .select(col('Nodes').cast(StringType()))\
        .coalesce(1) \
        .write \
        .format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .save(path_to_write + "/task3", header='false', sep=',')


def node_probabilities(toNodes, rank):
    # calculate no of out-links for current from node
    no_of_links = len(toNodes)

    # yield to node and probability paris
    for toNode in toNodes:
        yield toNode, rank/no_of_links


def page_rank(df_l):
   # extract unique links as tuples and group them by their from node
    rdd_links = df_l\
        .rdd\
        .map(lambda r: (r[0], r[1]))\
        .distinct()\
        .groupByKey()\
        .cache()

    # initialize all unique from nodes with an initial rank 1.0
    rdd_ranks = rdd_links.map(lambda x: (x[0], 1.0))

    # update the node ranks using page rank algorithm
    for i in range(no_of_updates):
        # calculate the probability distribution per from node
        rdd_links_ranks = rdd_links\
            .join(rdd_ranks) \
            .flatMap(lambda x: node_probabilities(x[1][0], x[1][1]))

        # use the probability distribution to update the node ranks
        rdd_ranks = rdd_links_ranks\
            .reduceByKey(lambda x, y: x + y)\
            .mapValues(lambda x: x * weight + offset)

    # create dataframe of resulting ranks
    df_ranks = rdd_ranks\
        .toDF()\
        .toDF('Node', 'Rank')\
        .sort(col('Rank').desc())

    # write ranks to file
    df_ranks \
        .coalesce(1) \
        .write \
        .format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .save(path_to_write + "/task4", header='false', sep=',')

#########################################################################################################


if run_spark_in_cluster:
    spark = SparkSession.builder.appName('hw5').master('spark://spark-master:7077')
else:
    spark = SparkSession.builder.appName('hw5').master('local')

spark = spark.config("spark.sql.broadcastTimeout", "36000").getOrCreate()
sqlContext = SQLContext(spark)

# read in the data from text file and parse from and to node IDs
df_links = spark.read.csv(dataset_path, sep='\t', comment="#", inferSchema=True).toDF('FromNodeId', 'ToNodeId')

# Task 1: Given the Google web graph dataset, please output the list of web pages with the number of outlinks,
# sorted in descending order of the out-degrees.
web_pages_sorted_by_outlinks(df_links)

# Task 2: Please output the inlink distribution of the top linked web pages,
# sorted in descending order of the in-degrees.
web_pages_sorted_by_inlinks(df_links)

# Task 3: Design an algorithm that maintains the connectivity of two nodes in an efficient way.
# Given a node v, please output the list of nodes that v points to, and the list of nodes that points to v.
node_connectivity(df_links, task3_node)

# Task 4: Compute the PageRank of the graph using MapReduce.
page_rank(df_links)
