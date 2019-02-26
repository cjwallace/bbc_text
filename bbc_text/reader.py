import pyspark.sql.types as typ
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def spark_session():
    """Return a SparkSession."""
    conf = SparkConf()
#    conf.set("spark.executor.memory", "2g")
#    conf.set("spark.cores.max", "1")
    conf.set("spark.app.name", "bbc_text")
    spark = (
        SparkSession
        .builder
        .master('yarn')
        .config(conf = conf)
        .getOrCreate()
    )
    return spark


def read_articles(sc, datadir, category):
    """Takes a SparkContext, data directory name, and subfolder (category)
       name, and returns an RDD of (filename, text) pairs.
       Requires that articles are stored in HDFS in individual .txt files in
       subdirectories with the category name."""
    path = datadir + '/' + category
    articles_rdd = sc.wholeTextFiles(datadir + '/' + category)
    return articles_rdd


def create_article_df(spark, articles_rdd, category):
    """Create a DataFrame, given a RDD of (filename, text) pairs."""
    
    schema = typ.StructType(
        [typ.StructField('filename', typ.StringType(), False),
         typ.StructField('text', typ.StringType(), True)]
    )
    
    df = (
        spark
        .createDataFrame(articles_rdd, schema)
        .withColumn('category', F.lit(category))
    )
    
    return df

# # Building the program

# 1. create spark context
# 2. read many text files from a directory
# 3. combine them into a df
# 4. repeat for several directories
# 5. combine all directories into dataframe
# 6. write to hdfs

# ## Create spark context
#spark = spark_session()
#sc = spark.sparkContext

# ## Read text files from directory

#hdfs_directory = 'bbc_text/bbc/'
#targets = ['business', 'entertainment', 'politics', 'sport', 'tech']
#
#tech = spark.read.text(hdfs_directory + targets[4] + '/400.txt')
#all_tech = sc.wholeTextFiles(hdfs_directory + targets[4])
#
#articles_rdd = read_articles(sc, hdfs_directory, 'tech')
#
#
#def df_from_articles(sc, articles_rdd, category):
#    """Create a DataFrame, given the date directory name for a collection
#       of plaintext articles in .txt format."""
#    
#    schema = typ.StructType(
#        [typ.StructField('filename', typ.StringType(), False),
#         typ.StructField('text', typ.StringType(), True)]
#    )
#    
#    df = (
#        spark
#            .createDataFrame(articles_rdd, schema)
#            .withColumn('category', F.lit(category))
#    )
#    
#    return df
#
#tech = df_from_articles(sc, articles_rdd, 'tech')