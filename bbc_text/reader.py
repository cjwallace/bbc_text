import pyspark.sql.types as typ
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def spark_session():
    """Return a SparkSession."""
    conf = SparkConf()
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

