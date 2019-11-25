from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
import pyspark.sql.functions as func
from pyspark.sql.functions import isnull, when, count, col

conf = SparkConf().setAppName('localTest')
# conf = SparkConf().setAppName('app').setMaster('spark://spark-master:7077').setSparkHome('/spark/')
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.getOrCreate()

transactions = spark.read.csv('../data/train_transaction.csv', header=True, inferSchema=True)
identities = spark.read.csv('../data/train_identity.csv', header=True, inferSchema=True)
txs = transactions.join(identities, 'TransactionID', how='left').alias('txs')
txs = txs.sample(False, 0.1, 42)

number_of_txs = txs.count()


# Feature engineering, based on https://www.kaggle.com/artgor/eda-and-models

def add_mean_col(txs, group_by_col, mean_col):
    avg_col_name = 'avg(' + mean_col + ')'
    means = txs.groupBy(group_by_col).mean(mean_col)
    txs = txs.join(means, group_by_col, how='left')
    txs = txs.withColumn(mean_col + '_to_mean_' + group_by_col, txs[mean_col] / txs[avg_col_name])
    txs = txs.drop(avg_col_name)
    print("Finished: ", avg_col_name)
    return txs


def add_std_col(txs, group_by_col, std_col):
    std_col_name = 'stddev_samp(' + std_col + ')'
    means = txs.groupBy(group_by_col).agg(func.stddev(txs[std_col]))
    txs = txs.join(means, group_by_col, how='left')
    txs = txs.withColumn(std_col + '_to_std_' + group_by_col, txs[std_col] / txs[std_col_name])
    txs = txs.drop(std_col_name)
    print("Finished: ", std_col_name)
    return txs


txs = add_mean_col(txs, "card1", 'TransactionAmt')
txs = add_mean_col(txs, 'card4', 'TransactionAmt')
txs = add_std_col(txs, 'card1', 'TransactionAmt')
txs = add_std_col(txs, 'card4', 'TransactionAmt')

txs = add_mean_col(txs, "card1", 'id_02')
txs = add_mean_col(txs, 'card4', 'id_02')
txs = add_std_col(txs, 'card1', 'id_02')
txs = add_std_col(txs, 'card4', 'id_02')

txs = add_mean_col(txs, "card1", 'D15')
txs = add_mean_col(txs, 'card4', 'D15')
txs = add_std_col(txs, 'card1', 'D15')
txs = add_std_col(txs, 'card4', 'D15')

txs = add_mean_col(txs, "addr1", 'D15')
txs = add_mean_col(txs, 'addr2', 'D15')
txs = add_std_col(txs, 'addr1', 'D15')
txs = add_std_col(txs, 'addr2', 'D15')

# Prepare data for modeling, based on https://www.kaggle.com/artgor/eda-and-models

# many_null_cols + big_top_value_cols + one_value_cols
cols_to_drop = ['V301', 'id_22', 'V113', 'V320', 'V133', 'D7', 'V112', 'V24', 'V281', 'V86',
                'R_emaildomain_3', 'V124', 'V319', 'V284', 'V111', 'V25', 'id_24', 'V102', 'V108',
                'V103', 'V286', 'V123', 'V318', 'id_25', 'V89', 'V106', 'V77', 'id_18', 'id_26', 'V107',
                'V68', 'V23', 'V121', 'V115', 'V290', 'V88', 'V300', 'id_27', 'V295', 'V26', 'V305',
                'id_08', 'V118', 'id_07', 'V296', 'V117', 'C3', 'V101', 'V27', 'V129', 'V66', 'V293', 'V132', 'V137',
                'V309', 'V98', 'V125', 'V136', 'P_emaildomain_3', 'dist2', 'V116', 'V298', 'V135', 'V120', 'V65',
                'V109', 'id_21', 'V134', 'V55', 'V105', 'V311', 'V316', 'V14', 'V28', 'V114', 'V110', 'V104', 'V119',
                'id_23', 'V67', 'V122', 'V299', 'V297', 'V321']

txs = txs.drop(*cols_to_drop)
print("Dropped: ", cols_to_drop)

cat_cols = ['id_12', 'id_13', 'id_14', 'id_15', 'id_16', 'id_17', 'id_18', 'id_19', 'id_20', 'id_21', 'id_22', 'id_23',
            'id_24', 'id_25', 'id_26', 'id_27', 'id_28', 'id_29',
            'id_30', 'id_31', 'id_32', 'id_33', 'id_34', 'id_35', 'id_36', 'id_37', 'id_38', 'DeviceType', 'DeviceInfo',
            'ProductCD', 'card4', 'card6', 'M4', 'P_emaildomain',
            'R_emaildomain', 'card1', 'card2', 'card3', 'card5', 'addr1', 'addr2', 'M1', 'M2', 'M3', 'M5', 'M6', 'M7',
            'M8', 'M9',
            'P_emaildomain_1', 'P_emaildomain_2', 'P_emaildomain_3', 'R_emaildomain_1', 'R_emaildomain_2',
            'R_emaildomain_3']

for col in cat_cols:
    if col in txs.columns:
        indexer = StringIndexer(inputCol=col, outputCol=col +"indexed")
        txs = indexer.fit(txs).transform(txs)